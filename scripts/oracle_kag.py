#!/usr/bin/env python3
"""
oracle_kag.py
----------------------------------------
Keyword-Augmented Generation (KAG) oracle pre-pipeline

This script:
  1. Parses Markdown stories into clean text files (stories_out/)
  2. Uses a local MLX model to infer emotional hashtags for each story
  3. Normalizes and reinforces hashtags across the corpus
  4. Builds a symlinked hashtag index (hashtags/)
  5. Writes a unified story_hashtags.jsonl for downstream dataset builders

Inputs:
  jim.md (or other Markdown file path from cfg["hashtagger"]["input_md"])

Outputs:
  stories_out/
  hashtags/
  story_hashtags.jsonl

Config integration:
  Reads model + prompt templates from load_config(), e.g.:

  hashtagger:
    model: microsoft/Phi-3-mini-4k-instruct
    input_md: jim.md
    prompts:
      emotional: "List the {num_tags} most important emotions in the story."
    num_tags: 10
    max_tokens: 200
    reinforce:
      top_n: 15
      min_global: 2
"""

import os
import re
import json
import html
import yaml
import time
from pathlib import Path
from collections import Counter
from mlx_lm import load, generate

# ---------------------------
# CONFIG LOADER
# ---------------------------
from config_loader import load_config

# ---------------------------
# MARKDOWN PARSER
# ---------------------------
def clean_markdown_text(text):
    text = text.replace("{{{First Name}}}", "friend")
    text = html.unescape(text)
    text = re.sub(r"\[([^\]]+)\]\[\d+\]", r"\1", text)
    text = re.sub(r"\[\d+\]", "", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"[_*]{1,3}([^*_]+)[_*]{1,3}", r"\1", text)
    text = re.sub(r"\s*\n\s*", " ", text)
    text = re.sub(r" {2,}", " ", text)
    return text.strip()

def safe_dirname(name):
    return re.sub(r"[^a-zA-Z0-9_-]", "_", name)[:50]

def save_story(outdir, title, text):
    dirname = Path(outdir) / safe_dirname(title)
    dirname.mkdir(parents=True, exist_ok=True)
    (dirname / "story.txt").write_text(text, encoding="utf-8")

def parse_markdown(md_path, outdir="stories_out"):
    Path(outdir).mkdir(exist_ok=True)
    with open(md_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    stories = {}
    current_title, buffer = None, []
    for line in lines:
        if line.startswith("# "):
            if current_title:
                full_text = "\n".join(buffer).strip()
                cleaned = clean_markdown_text(full_text)
                stories[current_title] = cleaned
                save_story(outdir, current_title, cleaned)
                buffer = []
            current_title = line[2:].strip()
        else:
            buffer.append(line.strip())

    if current_title and buffer:
        cleaned = clean_markdown_text("\n".join(buffer))
        stories[current_title] = cleaned
        save_story(outdir, current_title, cleaned)

    return stories

# ---------------------------
# ORACLE LLM QUERY
# ---------------------------
def query_llm_inprocess(model, tokenizer, story_text, prompt_template, num_tags=10, max_tokens=200):
    prompt = prompt_template.format(num_tags=num_tags) + "\n\nStory:\n" + story_text[:3000]
    response = generate(model=model, tokenizer=tokenizer, prompt=prompt, max_tokens=max_tokens)
    tags = [t.strip(" -•#") for t in response.splitlines() if t.strip()]
    return tags[:num_tags]

# ---------------------------
# HASHTAG NORMALIZATION + REINFORCEMENT
# ---------------------------
def normalize_tag(tag):
    t = tag.lstrip("#").strip()
    t = re.sub(r"(.)\1{2,}", r"\1\1", t)
    t = re.sub(r"[^A-Za-z0-9_-]", "", t)
    if not t or len(t) < 3 or len(t) > 30:
        return None
    return "#" + t.lower()

def reinforce_hashtags(all_story_tags, top_n=15, min_global=2):
    global_counts = Counter(tag for tags in all_story_tags for tag in tags)
    reinforced = []
    for tags in all_story_tags:
        local_counts = Counter(tags)
        scored = {t: local_counts[t] + 0.5 * global_counts[t] for t in tags}
        scored = {t: s for t, s in scored.items() if global_counts[t] >= min_global}
        top_tags = sorted(scored, key=scored.get, reverse=True)[:top_n]
        reinforced.append(top_tags)
    return reinforced

def save_story_tags(outdir, title, tags):
    dirname = Path(outdir) / safe_dirname(title)
    with open(dirname / "hashtags.json", "w", encoding="utf-8") as f:
        json.dump({"title": title, "hashtags": tags}, f, indent=2, ensure_ascii=False)

def build_hashtag_index(stories_dir="stories_out", hashtags_dir="hashtags"):
    base = Path(stories_dir)
    tagbase = Path(hashtags_dir)
    tagbase.mkdir(exist_ok=True)
    for story_dir in base.iterdir():
        if not story_dir.is_dir():
            continue
        tags_file = story_dir / "hashtags.json"
        story_file = story_dir / "story.txt"
        if not tags_file.exists() or not story_file.exists():
            continue
        with open(tags_file, "r", encoding="utf-8") as f:
            tags = json.load(f)["hashtags"]
        for tag in tags:
            tagdir = tagbase / tag
            tagdir.mkdir(parents=True, exist_ok=True)
            linkpath = tagdir / f"{story_dir.name}.txt"
            if not linkpath.exists():
                try:
                    os.symlink(story_file.resolve(), linkpath)
                except FileExistsError:
                    pass

# ---------------------------
# MAIN
# ---------------------------
def main():
    start = time.time()
    cfg = load_config()
    ROOT = Path(os.getenv("EXEC", Path(__file__).parent)).resolve()
    os.chdir(ROOT)

    model_name = cfg["hashtagger"]["model"]
    input_md = cfg["hashtagger"].get("input_md", "jim.md")
    emotional_prompt = cfg["hashtagger"]["prompts"]["emotional"]
    num_tags = int(cfg["hashtagger"].get("num_tags", 10))
    max_tokens = int(cfg["hashtagger"].get("max_tokens", 200))
    reinforce_cfg = cfg["hashtagger"].get("reinforce", {"top_n": 15, "min_global": 2})

    print(f"=== Oracle KAG starting ===")
    print(f"Model: {model_name}")
    print(f"Input: {input_md}")

    model, tokenizer = load(model_name)

    stories = parse_markdown(input_md, outdir="stories_out")
    all_story_tags = []

    for title, text in stories.items():
        emotional_tags = query_llm_inprocess(
            model,
            tokenizer,
            text,
            emotional_prompt,
            num_tags=num_tags,
            max_tokens=max_tokens
        )

        cleaned = sorted(
            set(filter(None, (normalize_tag(t) for t in emotional_tags)))
        )
        save_story_tags("stories_out", title, cleaned)
        all_story_tags.append(cleaned)
        print(f"{title} => {', '.join(cleaned)}")

    # Reinforce across corpus
    reinforced = reinforce_hashtags(
        all_story_tags,
        top_n=int(reinforce_cfg["top_n"]),
        min_global=int(reinforce_cfg["min_global"])
    )

    for title, tags in zip(stories.keys(), reinforced):
        save_story_tags("stories_out", title, tags)
        print(f"🔗 Reinforced {title} => {', '.join(tags)}")

    # Unified JSONL
    out_jsonl = Path("story_hashtags.jsonl")
    with open(out_jsonl, "w", encoding="utf-8") as fout:
        for title in stories.keys():
            data = json.load(open(Path("stories_out") / safe_dirname(title) / "hashtags.json"))
            fout.write(json.dumps(data, ensure_ascii=False) + "\n")

    build_hashtag_index("stories_out", "hashtags")

    dur = time.time() - start
    print(f"✅ Oracle KAG complete in {dur:.1f}s")
    print("✅ Outputs:")
    print("  • stories_out/")
    print("  • hashtags/")
    print(f"  • {out_jsonl}")

if __name__ == "__main__":
    main()
