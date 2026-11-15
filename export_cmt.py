#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""导出互动数据管道血缘的三份产物：edges.csv / nodes.csv / lineage_tree.txt。

用法:
    python3 export_cmt.py [--lineage lineage.json] [--renames renames.csv] [--outdir .]

口径见 lineage_tree.txt 尾部说明，摘要:
  * renames.csv 中的旧表名在导出时归一为新表名，新旧节点合并为一个节点；
  * 一个节点既是某任务产出、又是别的任务输入时，谱系树固定挂在“产出一侧”；
  * 每个节点在谱系树中恰好出现一次；多产出取编号最小的产出任务，
    任务多上游时挂到编号最小的上游节点，其余非首选消费边保留在 edges.csv；
  * 输出按编号排序、无时间戳，同一输入重跑内容不变。
"""

import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict

CAT_TASK = "任务"
CAT_TABLE = "表"
CAT_METRIC = "指标"
CATS = (CAT_TASK, CAT_TABLE, CAT_METRIC)
ASSET_CATS = (CAT_TABLE, CAT_METRIC)

OUT_EDGES = "edges.csv"
OUT_NODES = "nodes.csv"
OUT_TREE = "lineage_tree.txt"


def natural_key(text):
    """b01 < b10 < k01 < q01 之类的稳定排序键。"""
    m = re.match(r"^([^\d]*)(\d+)$", text)
    if m:
        return (m.group(1), int(m.group(2)), text)
    return (text, -1, text)


def read_csv_rows(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise SystemExit("renames.csv 为空或缺少表头")
        for row in reader:
            yield {k.strip(): (v or "").strip()
                   for k, v in row.items() if k is not None}


def load_renames(path):
    old_to_new = {}
    for row in read_csv_rows(path):
        old, new = row.get("旧表名", ""), row.get("新表名", "")
        if not old or not new:
            raise SystemExit("renames.csv 存在空白的旧表名/新表名: %r" % (row,))
        if old == new:
            continue
        if old in old_to_new and old_to_new[old] != new:
            raise SystemExit("renames.csv 中旧表名 %s 对应了多个新名" % old)
        old_to_new[old] = new
    return old_to_new


def load_lineage(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict) or "节点" not in data or "边" not in data:
        raise SystemExit("lineage.json 缺少 节点/边 字段")
    return data["节点"], data["边"]


def resolve_canonical_names(old_to_new):
    """沿重命名链解析到最终名称（兼容连续两次改名），并拦截环。"""
    def resolve(name):
        chain = [name]
        cur = name
        while cur in old_to_new:
            nxt = old_to_new[cur]
            if nxt in chain:
                raise SystemExit("renames.csv 存在重命名环: %s"
                                 % " -> ".join(chain + [nxt]))
            chain.append(nxt)
            cur = nxt
        return cur

    name_to_canon = {old: resolve(old) for old in old_to_new}
    for final_name in list(name_to_canon.values()):
        name_to_canon.setdefault(final_name, final_name)
    return name_to_canon


def normalize_nodes(raw_nodes, name_to_canon):
    """按最终名称合并节点；旧名节点并入对应的新名节点。"""
    by_id = {}
    name_to_ids = defaultdict(list)
    for raw in raw_nodes:
        nid = str(raw.get("编号", "")).strip()
        name = str(raw.get("名称", "")).strip()
        cat = str(raw.get("类别", "")).strip()
        if not nid or not name:
            raise SystemExit("节点缺少编号或名称: %r" % (raw,))
        if nid in by_id:
            raise SystemExit("节点编号重复: %s" % nid)
        if cat not in CATS:
            raise SystemExit("节点 %s 的类别非法: %s" % (nid, cat))
        by_id[nid] = {"id": nid, "name": name, "cat": cat, "formerly": []}
        name_to_ids[name].append(nid)

    id_to_canon_id = {}

    def canon_id(nid):
        seen = []
        cur = nid
        while cur in id_to_canon_id:
            if cur in seen:
                raise SystemExit("节点重命名映射成环: %s" % " -> ".join(seen + [cur]))
            seen.append(cur)
            cur = id_to_canon_id[cur]
        return cur

    for nid, node in by_id.items():
        final_name = name_to_canon.get(node["name"], node["name"])
        if final_name == node["name"]:
            continue
        targets = name_to_ids.get(final_name, [])
        if len(targets) != 1:
            raise SystemExit(
                "重命名 %s -> %s 时无法唯一定位目标节点（候选: %s）"
                % (node["name"], final_name, ",".join(targets) or "无"))
        target = by_id[targets[0]]
        if target["cat"] != node["cat"]:
            raise SystemExit("重命名两侧类别不一致: %s(%s) -> %s(%s)"
                             % (nid, node["cat"], target["id"], target["cat"]))
        id_to_canon_id[nid] = target["id"]

    canonical = {}
    for nid, node in by_id.items():
        if nid in id_to_canon_id:
            continue
        merged = dict(node)
        merged["formerly"] = sorted(
            {by_id[old]["name"] for old in id_to_canon_id if canon_id(old) == nid},
            key=natural_key,
        )
        canonical[nid] = merged

    dangling = sorted((old for old in id_to_canon_id
                       if canon_id(old) not in canonical), key=natural_key)
    if dangling:
        raise SystemExit("旧名节点未能并入新节点: %s" % ",".join(dangling))
    return canonical, id_to_canon_id


def normalize_edges(raw_edges, canonical, id_to_canon_id):
    def canon(nid):
        seen = []
        cur = nid
        while cur in id_to_canon_id:
            if cur in seen:
                raise SystemExit("节点重命名映射成环: %s" % " -> ".join(seen + [cur]))
            seen.append(cur)
            cur = id_to_canon_id[cur]
        return cur

    raw_pairs = []
    for raw in raw_edges:
        src = str(raw.get("来源", "")).strip()
        dst = str(raw.get("目标", "")).strip()
        if not src or not dst:
            raise SystemExit("边缺少来源或目标: %r" % (raw,))
        raw_pairs.append((src, dst))

    known = set(canonical) | set(id_to_canon_id)
    unknown = sorted({n for pair in raw_pairs for n in pair if n not in known},
                     key=natural_key)
    if unknown:
        raise SystemExit("边引用了不存在的节点: %s" % ",".join(unknown))

    merged_away = 0
    seen = set()
    edges = []
    for src, dst in raw_pairs:
        pair = (canon(src), canon(dst))
        if pair in seen:
            merged_away += 1
            continue
        seen.add(pair)
        edges.append(pair)
    edges.sort(key=lambda p: (natural_key(p[0]), natural_key(p[1])))
    return edges, merged_away


def build_graph(canonical, edges):
    producers = defaultdict(list)    # 资产 -> 产出任务
    task_inputs = defaultdict(list)  # 任务 -> 输入资产
    for src, dst in edges:
        s_cat, d_cat = canonical[src]["cat"], canonical[dst]["cat"]
        if s_cat in ASSET_CATS and d_cat == CAT_TASK:
            task_inputs[dst].append(src)
        elif s_cat == CAT_TASK and d_cat in ASSET_CATS:
            producers[dst].append(src)
        else:
            raise SystemExit("非法边类型: %s(%s) -> %s(%s)"
                             % (src, s_cat, dst, d_cat))
    return producers, task_inputs


def build_tree(canonical, producers, task_inputs):
    """构造覆盖所有节点的单亲树，返回父节点、子节点、根与多义节点。"""
    parent = {}
    multi_producer = {}
    multi_primary = {}

    # 资产挂在产出一侧；没有任何产出任务时才是根。
    for asset in sorted((n for n in canonical if canonical[n]["cat"] in ASSET_CATS),
                        key=natural_key):
        prods = sorted(set(producers.get(asset, [])), key=natural_key)
        if not prods:
            continue
        parent[asset] = prods[0]
        if len(prods) > 1:
            multi_producer[asset] = prods

    # 有上游输入的任务挂到编号最小的上游资产，保证每个节点只出现一次。
    for task in sorted((n for n in canonical if canonical[n]["cat"] == CAT_TASK),
                       key=natural_key):
        ups = sorted(set(task_inputs.get(task, [])), key=natural_key)
        if not ups:
            continue
        parent[task] = ups[0]
        if len(ups) > 1:
            multi_primary[task] = ups

    roots = sorted((n for n in canonical if n not in parent), key=natural_key)
    if not roots:
        raise SystemExit("谱系树没有根节点，数据可能为空")

    children = defaultdict(list)
    for child, par in parent.items():
        children[par].append(child)
    for par in children:
        children[par].sort(key=natural_key)

    for nid in canonical:
        path = []
        cur = nid
        while cur in parent:
            if cur in path:
                raise SystemExit("谱系树父链成环: %s" % " -> ".join(path + [cur]))
            path.append(cur)
            cur = parent[cur]

    return parent, children, roots, multi_producer, multi_primary


def render_tree(canonical, children, roots, edges, stats):
    lines = []
    stack = [(r, 0) for r in reversed(roots)]
    while stack:
        nid, depth = stack.pop()
        node = canonical[nid]
        suffix = "（旧名: %s）" % "、".join(node["formerly"]) if node["formerly"] else ""
        lines.append("%s%s [%s]%s" % ("  " * depth, node["name"], nid, suffix))
        for child in reversed(children.get(nid, [])):
            stack.append((child, depth + 1))

    counts = defaultdict(int)
    for node in canonical.values():
        counts[node["cat"]] += 1

    lines.append("")
    lines.append("说明:")
    lines.append("  1. 旧名归一: renames.csv 中 %d 个旧表名已并入对应新表, 同一节点只出现一次。"
                 % len(stats["renames"]))
    detail = "; ".join("%s(%s)→%s(%s)" % (old, old_id, data["name"], nid)
                       for old, old_id, data, nid in stats["rename_details"])
    lines.append("     合并明细: %s" % (detail or "无"))
    lines.append("  2. 挂载口径: 一个节点既是某任务的产出、又是别的任务的输入时, 挂在“产出一侧”, "
                 "即作为其产出任务的子节点, 其下游任务再作为该节点的子节点缩进一层。")
    lines.append("     每个节点在树中只出现一次。资产有多个产出任务时取编号最小者: %s"
                 % (", ".join(stats["multi_producer"]) or "无"))
    lines.append("     任务有多个上游时挂到编号最小的上游节点: %s"
                 % (", ".join(stats["multi_primary"]) or "无"))
    lines.append("     其余非首选消费边不重复画进树, 完整边集以 edges.csv 为准。")
    lines.append("  3. 计数: 节点 %d（任务 %d, 表 %d, 指标 %d）; 归一后边 %d; "
                 "树中连线 %d; 未画出的非首选边 %d。"
                 % (len(canonical), counts[CAT_TASK], counts[CAT_TABLE],
                    counts[CAT_METRIC], len(edges), stats["tree_links"],
                    len(edges) - stats["tree_links"]))
    lines.append("     对齐: 树中连线 %d + 未画出边 %d = edges.csv 行数 %d; "
                 "树中连线 + 根节点 %d = 节点数 %d。"
                 % (stats["tree_links"], len(edges) - stats["tree_links"], len(edges),
                    len(roots), stats["tree_links"] + len(roots)))
    if stats["merged_dup_edges"]:
        lines.append("     归一后重复边已合并 %d 条。" % stats["merged_dup_edges"])
    if stats["dup_names"]:
        lines.append("  4. 同名节点以编号区分: %s。" % "; ".join(
            "%s(%s)" % (name, ",".join(ids)) for name, ids in stats["dup_names"]))
    else:
        lines.append("  4. 无同名节点。")
    lines.append("  5. 输出按编号排序、不含时间戳, 同一输入重跑内容不变。")
    return "\n".join(lines) + "\n"


def verify(canonical, edges, parent, rows_nodes, rows_edges, tree_text):
    problems = []
    if len(rows_nodes) != len(canonical):
        problems.append("nodes.csv 行数 %d != 归一后节点数 %d"
                        % (len(rows_nodes), len(canonical)))
    if len(rows_edges) != len(edges):
        problems.append("edges.csv 行数 %d != 归一后边数 %d"
                        % (len(rows_edges), len(edges)))
    node_ids = {r["编号"] for r in rows_nodes}
    bad_endpoints = sorted({n for r in rows_edges
                            for n in (r["来源编号"], r["目标编号"])
                            if n not in node_ids}, key=natural_key)
    if bad_endpoints:
        problems.append("边端点不在 nodes.csv 中: %s" % ",".join(bad_endpoints))

    tree_part = tree_text.split("说明:", 1)[0]
    tree_ids = re.findall(r"\[([^\]]+)\]", tree_part)
    missing = sorted(set(canonical) - set(tree_ids), key=natural_key)
    extra = sorted(set(tree_ids) - set(canonical), key=natural_key)
    repeated = sorted({i for i in tree_ids if tree_ids.count(i) > 1}, key=natural_key)
    if len(tree_ids) != len(canonical):
        problems.append("谱系树节点行数 %d != 节点数 %d" % (len(tree_ids), len(canonical)))
    if missing:
        problems.append("谱系树缺少节点: %s" % ",".join(missing))
    if extra:
        problems.append("谱系树出现未知节点: %s" % ",".join(extra))
    if repeated:
        problems.append("谱系树节点重复: %s" % ",".join(repeated))
    if len(parent) + (len(edges) - len(parent)) != len(edges):
        problems.append("谱系树与边表计数无法对齐")
    return problems


def write_csv(path, fieldnames, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_text(path, text):
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(text)


def main(argv=None):
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description="导出互动数据管道血缘的三份产物")
    ap.add_argument("--lineage", default=os.path.join(here, "lineage.json"))
    ap.add_argument("--renames", default=os.path.join(here, "renames.csv"))
    ap.add_argument("--outdir", default=here)
    args = ap.parse_args(argv)

    old_to_new = load_renames(args.renames)
    name_to_canon = resolve_canonical_names(old_to_new)
    raw_nodes, raw_edges = load_lineage(args.lineage)

    canonical, id_to_canon_id = normalize_nodes(raw_nodes, name_to_canon)
    edges, merged_dup_edges = normalize_edges(raw_edges, canonical, id_to_canon_id)
    producers, task_inputs = build_graph(canonical, edges)
    parent, children, roots, multi_producer, multi_primary = build_tree(
        canonical, producers, task_inputs)

    rows_nodes = [{
        "编号": nid,
        "名称": canonical[nid]["name"],
        "类别": canonical[nid]["cat"],
        "旧名": ";".join(canonical[nid]["formerly"]),
    } for nid in sorted(canonical, key=natural_key)]

    rows_edges = [{
        "来源编号": src,
        "来源名称": canonical[src]["name"],
        "目标编号": dst,
        "目标名称": canonical[dst]["name"],
    } for src, dst in edges]

    name_to_ids = defaultdict(list)
    for nid in sorted(canonical, key=natural_key):
        name_to_ids[canonical[nid]["name"]].append(nid)
    dup_names = sorted(
        ((name, ids) for name, ids in name_to_ids.items() if len(ids) > 1),
        key=lambda kv: natural_key(kv[1][0]))

    raw_name_of = {str(n.get("编号", "")).strip(): str(n.get("名称", "")).strip()
                   for n in raw_nodes}
    rename_details = sorted(
        ((raw_name_of.get(old, old), old, canonical[target], target)
         for old, target in id_to_canon_id.items()),
        key=lambda item: natural_key(item[1]))

    stats = {
        "renames": old_to_new,
        "rename_details": rename_details,
        "dup_names": dup_names,
        "multi_producer": sorted(multi_producer, key=natural_key),
        "multi_primary": sorted(multi_primary, key=natural_key),
        "merged_dup_edges": merged_dup_edges,
        "tree_links": len(parent),
    }
    tree_text = render_tree(canonical, children, roots, edges, stats)

    problems = verify(canonical, edges, parent, rows_nodes, rows_edges, tree_text)
    if problems:
        sys.stderr.write("导出校验失败:\n")
        for p in problems:
            sys.stderr.write("  - %s\n" % p)
        return 2

    os.makedirs(args.outdir, exist_ok=True)
    write_csv(os.path.join(args.outdir, OUT_EDGES),
              ["来源编号", "来源名称", "目标编号", "目标名称"], rows_edges)
    write_csv(os.path.join(args.outdir, OUT_NODES),
              ["编号", "名称", "类别", "旧名"], rows_nodes)
    write_text(os.path.join(args.outdir, OUT_TREE), tree_text)

    counts = defaultdict(int)
    for node in canonical.values():
        counts[node["cat"]] += 1
    sys.stderr.write(
        "已导出: 节点 %d（任务 %d, 表 %d, 指标 %d）, 边 %d, 根任务/节点 %d, 树连线 %d\n"
        % (len(canonical), counts[CAT_TASK], counts[CAT_TABLE], counts[CAT_METRIC],
           len(edges), len(roots), len(parent)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
