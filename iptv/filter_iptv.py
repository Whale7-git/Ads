# -*- coding: utf-8 -*-
"""IPTV 源实测优选: 拉取多个公开源 -> 合并去重 -> 并发实测 -> 每频道保留最快N条 -> 输出 m3u/txt"""
import argparse
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
import ssl
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from urllib.parse import urljoin

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(BASE, "output")
os.makedirs(OUT, exist_ok=True)

SOURCES = [
    ("suxuang", "https://raw.githubusercontent.com/suxuang/myIPTV/main/ipv4.m3u"),
    ("vbskycn", "https://raw.githubusercontent.com/vbskycn/iptv/master/tv/iptv4.txt"),
    ("iptv-org", "https://iptv-org.github.io/iptv/countries/cn.m3u"),
]

UA_DEFAULT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
TIMEOUT = 4
KEEP_PER_CHANNEL = 3
WORKERS = 80
DEEP_TIMEOUT = 6
DEEP_WORKERS = 40

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


def fetch(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": UA_DEFAULT})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        return r.read().decode("utf-8", "ignore")


def parse_m3u(text, tag):
    """返回 [(name, group, url, ua)]"""
    items = []
    cur = None
    cur_group = ""
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#EXTINF"):
            name = line.rsplit(",", 1)[-1].strip() if "," in line else ""
            tvg = re.search(r'tvg-name="([^"]*)"', line)
            grp = re.search(r'group-title="([^"]*)"', line)
            ua = re.search(r'http-user-agent="([^"]*)"', line)
            cur = {
                "name": (tvg.group(1).strip() if tvg and tvg.group(1).strip() else name),
                "group": grp.group(1).strip() if grp else cur_group,
                "ua": ua.group(1).strip() if ua else "",
                "tag": tag,
            }
            continue
        if line.startswith("#"):
            # 形如 "#4K频道" 的分组标记
            if not line.startswith("#EXT") and len(line) < 30:
                cur_group = line.lstrip("#").strip()
            continue
        if cur is not None:
            cur["url"] = line
            if cur["name"] and cur["url"].startswith("http"):
                items.append(cur)
            cur = None
    return items


def parse_txt(text, tag):
    """央视,http://... 配合 分组,#genre#"""
    items = []
    group = ""
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "#genre#" in line:
            group = line.split(",")[0].strip()
            continue
        if "," not in line:
            continue
        name, url = line.split(",", 1)
        name, url = name.strip(), url.strip()
        if url.startswith("http") and name:
            items.append({"name": name, "group": group, "ua": "", "tag": tag, "url": url})
    return items


GROUP_MAP = {
    "general": "综合", "news": "新闻", "sports": "体育", "movies": "电影",
    "entertainment": "娱乐", "kids": "少儿", "music": "音乐", "documentary": "纪录",
    "education": "科教", "business": "财经", "culture": "文化", "lifestyle": "生活",
    "family": "家庭", "weather": "气象", "shop": "购物", "religious": "宗教",
    "undefined": "其他", "": "其他",
}


def norm_name(name):
    """频道名归一化, 用于跨合并"""
    s = name.strip()
    s = re.sub(r"\[[^\]]*ms\]", "", s)
    s = re.sub(r"\((?:720|1080|576|480|2160)[pi]\)", "", s, flags=re.I)
    s = re.sub(r"\b(?:HD|FHD|UHD|SD|4K|8K)\b", "", s, flags=re.I)
    s = re.sub(r"高清|超清|标清|蓝光", "", s)
    s = s.replace("-", "").replace("_", "").replace(" ", "").replace("·", "")
    s = s.replace("＋", "+").replace("(", "").replace(")", "").replace("（", "").replace("）", "")
    m = re.match(r"(?i)^(cctv|cgtn|cetv)(\d+[a-z+-]*)(.*)$", s)
    if m:
        s = f"{m.group(1).upper()}{m.group(2)}{m.group(3)}"
    return s or name.strip()


CN_MAP = {
    "电影": "电影", "体育": "体育", "纪录": "纪录", "纪录片": "纪录", "音乐": "音乐",
    "少儿": "少儿", "儿童": "少儿", "动画": "少儿", "新闻": "新闻", "综合": "综合",
    "科教": "科教", "科普": "科教", "生活": "生活", "文化": "文化", "电视剧": "电视剧",
    "其他": "其他", "卫视": "卫视频道", "央视": "央视频道",
}


def norm_group(g):
    g = (g or "").strip()
    g = re.sub(r"[☘️🍀✨⭐★☆•·]+", "", g).strip()
    key = g.lower().split(";")[0].strip()
    if key in GROUP_MAP:
        return GROUP_MAP[key]
    base = re.sub(r"频道$", "", re.sub(r"(?:HD|高清)$", "", key)).strip()
    if base in CN_MAP:
        return CN_MAP[base]
    return g or "其他"


def probe(item):
    """真实探测: 能否取到 HLS 内容"""
    url = item["url"]
    ua = item.get("ua") or UA_DEFAULT
    headers = {"User-Agent": ua, "Accept": "*/*"}
    req = urllib.request.Request(url, headers=headers)
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT, context=ctx) as r:
            ctype = (r.headers.get("Content-Type") or "").lower()
            body = r.read(4096)
        ms = int((time.time() - t0) * 1000)
        head = body[:400]
        text = head.decode("utf-8", "ignore")
        ok = False
        if r.status in (200, 206):
            if "#EXTM3U" in text or "#EXTINF" in text:
                ok = True
            elif ctype.startswith("video/") or "octet-stream" in ctype or "mpegurl" in ctype:
                ok = True and len(body) > 0
        item["ms"] = ms
        item["ok"] = ok
        item["reason"] = "" if ok else f"content-not-hls({ctype[:24]})"
        return item
    except urllib.error.HTTPError as e:
        item.update(ok=False, ms=-1, reason=f"http{e.code}")
        return item
    except Exception as e:
        item.update(ok=False, ms=-1, reason=type(e).__name__)
        return item


def _http_get(url, ua, rng=None, timeout=DEEP_TIMEOUT, max_bytes=262144):
    """有界读取: 裸 TS 直播流会无限推送, 必须限制读取字节数, 否则永久阻塞"""
    h = {"User-Agent": ua or UA_DEFAULT, "Accept": "*/*"}
    if rng:
        h["Range"] = rng
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        return r.status, r.read(max_bytes), (r.headers.get("Content-Type") or "")


def _first_uri(playlist_text):
    """取播放列表里第一条非注释行(变体或分片地址)"""
    for line in playlist_text.splitlines():
        s = line.strip()
        if s and not s.startswith("#"):
            return s
    return ""


def deep_check(item):
    """分片级校验: 顺着 m3u8 取到真实视频分片, 能拿到二进制数据才算可播"""
    cur = item["url"]
    ua = item.get("ua") or UA_DEFAULT
    try:
        st, body, ct = _http_get(cur, ua, max_bytes=131072)
        if st not in (200, 206):
            item.update(deep_ok=False, deep_reason=f"playlist-http{st}")
            return item
        text = body.decode("utf-8", "ignore")

        # master -> variant 最多下钻两层
        for _ in range(2):
            if "#EXT-X-STREAM-INF" not in text:
                break
            nxt = _first_uri(text)
            if not nxt:
                break
            cur = urljoin(cur, nxt)
            st, body, ct = _http_get(cur, ua, max_bytes=131072)
            if st not in (200, 206):
                item.update(deep_ok=False, deep_reason=f"variant-http{st}")
                return item
            text = body.decode("utf-8", "ignore")

        # media playlist -> 首个分片
        if "#EXTINF" in text:
            seg = _first_uri(text)
            if seg:
                cur = urljoin(cur, seg)

        st, seg_body, seg_ct = _http_get(cur, ua, rng="bytes=0-262144")
        if st not in (200, 206):
            item.update(deep_ok=False, deep_reason=f"seg-http{st}")
            return item
        if len(seg_body) < 1024:
            item.update(deep_ok=False, deep_reason=f"seg-tiny({len(seg_body)}B)")
            return item
        head = seg_body[:512].lower()
        if b"<html" in head or b"<xml" in head:
            item.update(deep_ok=False, deep_reason="seg-is-html")
            return item
        item.update(deep_ok=True, deep_reason="", seg_bytes=len(seg_body))
        return item
    except Exception as e:
        item.update(deep_ok=False, deep_reason=type(e).__name__)
        return item


def main():
    ap = argparse.ArgumentParser(description="IPTV 源实测优选")
    ap.add_argument("--deep", action="store_true",
                    help="对存活源做分片级深度校验, 剔除索引通但分片死的假活源")
    args = ap.parse_args()

    all_items = []
    for tag, url in SOURCES:
        try:
            txt = fetch(url)
            its = parse_m3u(txt, tag) if url.endswith(".m3u") else parse_txt(txt, tag)
            print(f"[源] {tag:10s} 解析 {len(its):5d} 条")
            all_items.extend(its)
        except Exception as e:
            print(f"[源] {tag:10s} 拉取失败: {e}")

    # 去重
    seen = set()
    uniq = []
    for it in all_items:
        k = it["url"]
        if k in seen:
            continue
        seen.add(k)
        uniq.append(it)
    print(f"[合并] 原始 {len(all_items)} 条 -> 去重后 {len(uniq)} 条, 开始实测...")

    results = []
    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(probe, it) for it in uniq]
        for f in as_completed(futs):
            results.append(f.result())
            done += 1
            if done % 200 == 0:
                alive = sum(1 for r in results if r["ok"])
                print(f"  ...{done}/{len(uniq)}  存活 {alive}")

    alive = [r for r in results if r["ok"]]
    print(f"[实测] 存活 {len(alive)} / {len(results)} ({len(alive)*100//max(1,len(results))}%)")

    if args.deep:
        print(f"[深度] 对 {len(alive)} 条做分片级校验...")
        d_done = 0
        with ThreadPoolExecutor(max_workers=DEEP_WORKERS) as ex:
            futs = [ex.submit(deep_check, it) for it in alive]
            for f in as_completed(futs):
                f.result()
                d_done += 1
                if d_done % 200 == 0:
                    print(f"  ...深度 {d_done}/{len(alive)}")
        passed = [it for it in alive if it.get("deep_ok")]
        print(f"[深度] 分片校验通过 {len(passed)} / {len(alive)}"
              f" (剔除 {len(alive)-len(passed)} 条假活源)")
        alive = passed

    # 按归一化频道名聚合, 保留最快的 N 条
    by_key = defaultdict(list)
    for r in alive:
        r["norm"] = norm_name(r["name"])
        r["cgroup"] = norm_group(r["group"])
        by_key[r["norm"]].append(r)

    picked = []
    for key, lst in by_key.items():
        lst.sort(key=lambda x: x["ms"])
        best_name = min(lst, key=lambda x: len(x["name"]))["name"]
        best_group = lst[0]["cgroup"]
        for it in lst[:KEEP_PER_CHANNEL]:
            it["disp"] = best_name
            it["cgroup"] = best_group
            picked.append(it)
    picked.sort(key=lambda x: (x["cgroup"], x["norm"], x["ms"]))

    # 输出 m3u
    m3u_path = os.path.join(OUT, "iptv.m3u")
    with open(m3u_path, "w", encoding="utf-8") as f:
        f.write('#EXTM3U x-tvg-url="http://epg.51zmt.top:8000/e.xml"\n')
        lastg = None
        for it in picked:
            g = it["cgroup"]
            if g != lastg:
                f.write(f"\n# {g}\n")
                lastg = g
            ua = f' http-user-agent="{it["ua"]}"' if it.get("ua") else ""
            f.write(f'#EXTINF:-1 group-title="{g}"{ua},{it["disp"]} [{it["ms"]}ms]\n{it["url"]}\n')

    # 输出 txt
    txt_path = os.path.join(OUT, "iptv.txt")
    with open(txt_path, "w", encoding="utf-8") as f:
        lastg = None
        for it in picked:
            g = it["cgroup"]
            if g != lastg:
                f.write(f"{g},#genre#\n")
                lastg = g
            f.write(f'{it["disp"]},{it["url"]}\n')

    # 统计
    groups = defaultdict(int)
    for it in picked:
        groups[it["cgroup"]] += 1
    lines = [
        "# IPTV 优选结果",
        "",
        f"- 生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- 总候选: {len(uniq)} 条（去重后）",
        f"- 实测存活: {len(alive)} 条 ({len(alive)*100//max(1,len(results))}%)"
        + (f"（已分片级校验）" if args.deep else ""),
        f"- 最终收录: {len(picked)} 条 / {len(by_key)} 个频道（每频道最多 {KEEP_PER_CHANNEL} 条，按延迟排序）",
        "",
        "## 分组分布",
        "",
        "| 分组 | 条数 |",
        "| --- | --- |",
    ]
    for g, c in sorted(groups.items(), key=lambda x: -x[1]):
        lines.append(f"| {g} | {c} |")
    with open(os.path.join(OUT, "report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    # 输出 TVBox 配置: TVBox 的"配置地址"只认 JSON, 不认 m3u
    raw_base = "https://raw.githubusercontent.com/Whale7-git/Ads/main/iptv/output"
    cfg = {
        "sites": [],
        "lives": [{
            "name": "IPTV优选",
            "type": 0,
            "url": f"{raw_base}/iptv.m3u",
            "playerType": 1,
            "epg": "http://epg.51zmt.top:8000/e.xml",
            "logo": "",
            "ua": "",
        }],
        "parses": [],
        "flags": [],
        "wallpaper": "",
        "warning": f"直播源每日自动刷新, 本次 {len(picked)} 条 / {len(by_key)} 个频道",
    }
    with open(os.path.join(OUT, "tvbox.json"), "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

    print(f"[完成] 频道 {len(by_key)} 个, 收录 {len(picked)} 条 -> {m3u_path}")
    for g, c in sorted(groups.items(), key=lambda x: -x[1])[:12]:
        print(f"   {g:16s} {c}")


if __name__ == "__main__":
    main()
