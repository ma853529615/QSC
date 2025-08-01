import os
import csv
import random
import argparse

def load_schema(path):
    """
    Load CSV files under `path`, using filenames as predicate names.
    Each CSV has 1 or 2 columns; first row is header. Subsequent rows are values.
    Returns a dict: { predicate: [ (val0, val1) ... ] }
    """
    schema = {}
    for fname in os.listdir(path):
        if not fname.endswith('.csv'):
            continue
        pred = os.path.splitext(fname)[0]
        rows = []
        with open(os.path.join(path, fname), newline='', encoding='utf-8') as fp:
            reader = csv.reader(fp)
            next(reader, None)  # skip header
            flag = True
            for row in reader:
                if len(row) == 1:
                    rows.append((row[0], None))
                    flag = False
                    break
                elif len(row) >= 2:
                    rows.append((row[0], row[1]))
        if flag and rows:
            schema[pred] = rows
    return schema


def format_const(value):
    # Quote non-numeric constants
    try:
        float(value)
        return value
    except:
        return f'"{value}"'


def ensure_variable_presence(args, var_prefix='V'):
    """
    Ensure at least one argument in list starts with variable prefix.
    If none, randomly replace one constant arg with a variable.
    """
    if any(a.startswith(var_prefix) or a in ('X', 'Y') for a in args):
        return args
    idx = random.randrange(len(args))
    # assign a variable name
    args[idx] = f"{var_prefix}{idx}" if var_prefix == 'V' else ('X' if idx == 0 else 'Y')
    return args


def gen_single(pred, schema):
    """
    Generate a single-hop triple ensuring non-empty result.
    Each term is variable with prob 2/3, constant with prob 1/3.
    Ensure at least one variable.
    Returns triple string.
    """
    s, o = random.choice(schema[pred])
    # decide term types
    arg0 = format_const(s) if (random.random() < 1/3 and s is not None) else 'X'
    arg1 = format_const(o) if (random.random() < 1/3 and o is not None) else 'Y'
    # ensure at least one variable
    args = ensure_variable_presence([arg0, arg1])
    return f"{pred}({args[0]}, {args[1]})"


def gen_chain(schema, length, max_trials=10):
    """
    Generate a chain of `length` hops ensuring a valid join path.
    Each term is variable with prob 2/3, constant with prob 1/3.
    Ensure at least one variable.
    Returns triple list string or None if fail.
    """
    for _ in range(max_trials):
        preds = random.sample(list(schema.keys()), length)
        # first hop
        row0 = random.choice(schema[preds[0]])
        path_vals = [row0[0], row0[1]]
        valid = True
        # subsequent hops
        for i in range(1, length):
            subj = path_vals[-1]
            candidates = [r for r in schema[preds[i]] if r[0] == subj]
            if not candidates:
                valid = False
                break
            row = random.choice(candidates)
            path_vals.append(row[1])
        if not valid or any(v is None for v in path_vals):
            continue
        # assign args with probability
        args = []
        for idx, val in enumerate(path_vals):
            if random.random() < 1/3:
                args.append(format_const(val))
            else:
                args.append(f'V{idx}')
        # ensure at least one variable in chain
        args = ensure_variable_presence(args, var_prefix='V')
        triples = [f"{preds[i]}({args[i]}, {args[i+1]})" for i in range(length)]
        return ", ".join(triples)
    return None

def generate_queries(path):
    """
    Generate queries in triple format with non-empty result guarantee:
    1. Single-hop coverage: one query per predicate.
    2. Multi-hop sampling: floor(R/3) valid chains (2 or 3 hops).
    3. Shuffle and write to generated_queried.sql.
    """
    schema = load_schema(path)
    all_preds = list(schema.keys())
    R = len(all_preds)
    queries = []

    # Single-hop coverage
    for pred in all_preds:
        queries.append(gen_single(pred, schema))

    # Multi-hop sampling
    M = R // 3
    count = 0
    while count < M:
        length = random.choice([2, 3])
        triple_list = gen_chain(schema, length)
        if triple_list and triple_list not in queries:
            queries.append(triple_list)
            count += 1

    # Shuffle
    random.shuffle(queries)

    # Output
    out_file = os.path.join(path, 'generated_queries_test.sql')
    with open(out_file, 'w', encoding='utf-8') as f:
        for q in queries:
            f.write(q + '\n')
    print(f"Wrote {len(queries)} queries to {out_file}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate triple-form queries from CSV dataset')
    base_path = "/NewData/mjh/KR/QC/QueryComp_1/datasets_csv"
    datasets = ["FB15K", "WN18", "wn2021", "nell", "NELL-500", "LUBM-1", "LUBM-10", "LUBM"]
    # datasets = ["nell"]
    for dataset in datasets:
        generate_queries(os.path.join(base_path, dataset))
