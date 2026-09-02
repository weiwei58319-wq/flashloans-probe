#!/usr/bin/env python3
"""
fb_probe.py — проба узла ПЕРЕД прогоном M7.

Отвечает на три вопроса, каждый — да/нет с числом:
  1. Отдаёт ли endpoint pendingLogs (пред-подтверждённые логи Flashblocks)?
  2. Отдаёт ли newFlashblocks?
  3. НА СКОЛЬКО РАНЬШЕ приходит один и тот же лог по pendingLogs,
     чем по обычной подписке logs из закрытого блока?

Пункт 3 — это и есть величина, которая двигает потолок готовности R.

Запуск (Colab / любая машина с исходящей сетью):
    pip install websockets
    python fb_probe.py wss://ВАШ_ENDPOINT --minutes 20 \
        --address 0xОРАКУЛ1 --address 0xОРАКУЛ2 --address 0xОРАКУЛ3

Часы: все интервалы считаются по МОНОТОННЫМ часам (time.monotonic).
Перевод системного времени на измерение не влияет — урок 2026-09-02.
"""

import argparse
import asyncio
import json
import statistics
import sys
import time

try:
    import websockets
except ImportError:
    sys.exit("нужен пакет websockets:  pip install websockets")


def log_key(ev):
    """Ключ, по которому один и тот же лог опознаётся в обоих потоках.

    blockHash/logIndex у пред-подтверждённого лога занулены или null,
    поэтому опознаём по txHash+topics+data. Если txHash тоже null —
    лог не парный, он идёт в счётчик unpaired, а не в статистику.
    """
    tx = ev.get("transactionHash") or ev.get("transactionhash")
    if not tx or tx == "0x" + "0" * 64:
        return None
    topics = ",".join(ev.get("topics") or [])
    return (tx.lower(), topics, ev.get("data", ""))


async def subscribe(ws, params, tag, sink, errors):
    req_id = abs(hash(tag)) % 100000
    await ws.send(json.dumps({"jsonrpc": "2.0", "id": req_id,
                              "method": "eth_subscribe", "params": params}))
    sub_id = None
    async for raw in ws:
        msg = json.loads(raw)
        if msg.get("id") == req_id:
            if "error" in msg:
                errors[tag] = msg["error"]
                return
            sub_id = msg.get("result")
            print(f"  [{tag}] подписка принята: {sub_id}")
            continue
        p = msg.get("params") or {}
        if sub_id and p.get("subscription") == sub_id:
            sink(p.get("result"), time.monotonic())


async def stream(url, params, tag, sink, errors, stop_at):
    """Отдельное соединение на каждую подписку: так одна не задерживает другую."""
    try:
        async with websockets.connect(url, ping_interval=20, max_size=None) as ws:
            await asyncio.wait_for(
                subscribe(ws, params, tag, sink, errors),
                timeout=max(1.0, stop_at - time.monotonic()))
    except asyncio.TimeoutError:
        pass
    except Exception as e:
        errors.setdefault(tag, f"{type(e).__name__}: {e}")


async def main(a):
    stop_at = time.monotonic() + a.minutes * 60
    flt = {}
    if a.address:
        flt["address"] = a.address if len(a.address) > 1 else a.address[0]
    if a.topic0:
        flt["topics"] = [a.topic0]

    pending, confirmed = {}, {}
    fb_count = [0]
    errors = {}

    def sink_pending(ev, t):
        if isinstance(ev, dict) and (k := log_key(ev)):
            pending.setdefault(k, t)

    def sink_confirmed(ev, t):
        if isinstance(ev, dict) and (k := log_key(ev)):
            confirmed.setdefault(k, t)

    def sink_fb(ev, t):
        fb_count[0] += 1

    print(f"Проба {a.minutes} мин. Фильтр: {json.dumps(flt) or '(без фильтра)'}\n")

    await asyncio.gather(
        stream(a.url, ["pendingLogs", flt] if flt else ["pendingLogs"],
               "pendingLogs", sink_pending, errors, stop_at),
        stream(a.url, ["logs", flt] if flt else ["logs"],
               "logs", sink_confirmed, errors, stop_at),
        stream(a.url, ["newFlashblocks"], "newFlashblocks", sink_fb, errors, stop_at),
    )

    print("\n" + "=" * 62)
    print("РЕЗУЛЬТАТ")
    print("=" * 62)

    for tag in ("pendingLogs", "logs", "newFlashblocks"):
        if tag in errors:
            print(f"  {tag:<16} НЕ ПОДДЕРЖИВАЕТСЯ / ошибка: {errors[tag]}")
        else:
            print(f"  {tag:<16} работает")

    print(f"\n  логов по pendingLogs : {len(pending)}")
    print(f"  логов по logs        : {len(confirmed)}")
    print(f"  сообщений newFlashblocks: {fb_count[0]}")

    paired = [(confirmed[k] - pending[k]) * 1000
              for k in pending.keys() & confirmed.keys()]

    if len(paired) >= 3:
        paired.sort()
        print(f"\n  ПАРНЫХ ЛОГОВ: {len(paired)}")
        print(f"  Опережение pendingLogs над закрытым блоком, мс:")
        print(f"    медиана {statistics.median(paired):8.0f}")
        print(f"    p10     {paired[len(paired)//10]:8.0f}")
        print(f"    p90     {paired[len(paired)*9//10]:8.0f}")
        print(f"    мин     {paired[0]:8.0f}   макс {paired[-1]:8.0f}")
        neg = sum(1 for x in paired if x < 0)
        if neg:
            print(f"    ВНИМАНИЕ: {neg} отрицательных — pendingLogs пришёл ПОЗЖЕ. "
                  f"Это дефект измерения, не результат.")
        print(f"\n  Это число — верхняя оценка того, на сколько вырастет запас\n"
              f"  до ликвидации, то есть чем оплачивается рост потолка R.")
    else:
        print(f"\n  Парных логов {len(paired)} — мало для вывода. "
              f"Увеличьте --minutes или ослабьте фильтр.")

    if fb_count[0]:
        rate = fb_count[0] / (a.minutes * 60)
        print(f"\n  Цена newFlashblocks: {rate:.1f} сообщ/с → "
              f"{rate*86400*30/1e6:.1f} млн запросов за 30 суток.")
    if pending:
        rate = len(pending) / (a.minutes * 60)
        print(f"  Цена pendingLogs   : {rate:.2f} сообщ/с → "
              f"{rate*86400*30/1e6:.3f} млн запросов за 30 суток.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("url", help="wss://... endpoint с поддержкой Flashblocks")
    p.add_argument("--minutes", type=float, default=20)
    p.add_argument("--address", action="append", default=[],
                   help="адрес оракула; можно повторять")
    p.add_argument("--topic0", default=None,
                   help="сигнатура события, напр. AnswerUpdated")
    asyncio.run(main(p.parse_args()))
