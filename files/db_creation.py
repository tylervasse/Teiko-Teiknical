import sqlite3
import pandas as pd

DATA_PATH = "cell-count.csv"
DB_PATH = "cell_counts.db"

ALL_COLUMNS = [
    "project",
    "subject",
    "condition",
    "age",
    "sex",
    "treatment",
    "response",
    "sample",
    "sample_type",
    "time_from_treatment_start",
    "b_cell", 
    "cd8_t_cell", 
    "cd4_t_cell", 
    "nk_cell", 
    "monocyte"
]

THE_SQL_SCRIPT = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS projects (
    project_id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS subjects (
    subject_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    condition TEXT,
    age INTEGER,
    sex TEXT,
    response TEXT,
    FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS treatments (
    treatment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS samples (
    sample_id TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL,
    treatment_id INTEGER,
    sample_type TEXT,
    time_from_treatment_start REAL,
    FOREIGN KEY(subject_id) REFERENCES subjects(subject_id) ON DELETE CASCADE,
    FOREIGN KEY(treatment_id) REFERENCES treatments(treatment_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS cell_populations (
    population_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS cell_counts (
    sample_id TEXT NOT NULL,
    population_id INTEGER NOT NULL,
    count INTEGER NOT NULL CHECK(count >= 0),
    PRIMARY KEY (sample_id, population_id),
    FOREIGN KEY(sample_id) REFERENCES samples(sample_id) ON DELETE CASCADE,
    FOREIGN KEY(population_id) REFERENCES cell_populations(population_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_subjects_project ON subjects(project_id);
CREATE INDEX IF NOT EXISTS idx_subjects_condition ON subjects(condition);
CREATE INDEX IF NOT EXISTS idx_samples_subject ON samples(subject_id);
CREATE INDEX IF NOT EXISTS idx_samples_treatment ON samples(treatment_id);
CREATE INDEX IF NOT EXISTS idx_subjects_response ON subjects(response);
CREATE INDEX IF NOT EXISTS idx_samples_time ON samples(time_from_treatment_start);
CREATE INDEX IF NOT EXISTS idx_cell_counts_population ON cell_counts(population_id);
CREATE INDEX IF NOT EXISTS idx_cell_counts_sample ON cell_counts(sample_id);
"""

def init_db(conn):
    conn.executescript(THE_SQL_SCRIPT)
    conn.commit()

def read_csv(file_path):
    df = pd.read_csv(file_path, sep=",")
    df.columns = [c.strip() for c in df.columns] # just in case
    return df

def load_data_from_csv_to_db(file_path, conn):
    df = read_csv(file_path)
    
    # missing data checker
    missing_data = [c for c in ALL_COLUMNS if c not in df.columns]
    if missing_data:
        raise Exception(
            "Missing required columns.\n"
            f"Missing data: {missing_data}\n"
            f"Found: {list(df.columns)}"
        )

    # stringify columns
    for col in ["project", "subject", "sample", "treatment"]:
        df[col] = df[col].astype(str)

    # inserting projects data
    for proj in df["project"].dropna().unique():
        conn.execute("INSERT OR IGNORE INTO projects(project_id) VALUES (?)", (proj,))
    conn.commit()

    # inserting subjects data
    subjects_df = df[["subject", "project", "condition", "age", "sex", "response"]].drop_duplicates(subset=["subject"]).copy()
    subjects_df["age"] = pd.to_numeric(subjects_df["age"], errors="coerce")
    subjects_df["age"] = subjects_df["age"].apply(lambda x: int(x) if pd.notna(x) else None)

    conn.executemany(
        "INSERT OR REPLACE INTO subjects(subject_id, project_id, condition, age, sex, response) VALUES (?, ?, ?, ?, ?, ?)",
        subjects_df.itertuples(index=False, name=None),
    )
    conn.commit()

    # inserting treatments data
    for t in df["treatment"].dropna().unique():
        conn.execute("INSERT OR IGNORE INTO treatments(name) VALUES (?)", (t,))
    conn.commit()
    
    treatment_map = dict(conn.execute("SELECT name, treatment_id FROM treatments").fetchall()) # updatable treatment map 

    # inserting samples data
    samples_df = df[["sample", "subject", "treatment", "sample_type", "time_from_treatment_start"]].copy()
    samples_df["time_from_treatment_start"] = pd.to_numeric(samples_df["time_from_treatment_start"], errors="coerce")
    samples_df["treatment_id"] = samples_df["treatment"].map(treatment_map)

    if samples_df["treatment_id"].isna().any():
        bad = samples_df[samples_df["treatment_id"].isna()]["treatment"].unique()
        raise Exception(f"Unmapped treatment(s): {bad}")

    conn.executemany(
        "INSERT OR REPLACE INTO samples(sample_id, subject_id, treatment_id, sample_type, time_from_treatment_start) VALUES (?, ?, ?, ?, ?)",
        samples_df[["sample", "subject", "treatment_id", "sample_type", "time_from_treatment_start"]]
            .itertuples(index=False, name=None),
    )
    conn.commit()
    

    # inserting cell_populations data
    for pop in ["b_cell", "cd8_t_cell", "cd4_t_cell", "nk_cell", "monocyte"]:
        conn.execute("INSERT OR IGNORE INTO cell_populations(name) VALUES (?)", (pop,))
    conn.commit()

    pop_map = dict(conn.execute("SELECT name, population_id FROM cell_populations").fetchall())

    # inserting cell_counts data
    counts_long = df[["sample", "b_cell", "cd8_t_cell", "cd4_t_cell", "nk_cell", "monocyte"]].melt(
        id_vars=["sample"],
        var_name="population_name",
        value_name="count",
    )

    counts_long["count"] = pd.to_numeric(counts_long["count"], errors="raise").astype(int)
    
    # negative counts checker
    if (counts_long["count"] < 0).any():
        bad = counts_long[counts_long["count"] < 0].head(10)
        raise Exception(f"Negative counts found (showing up to 10):\n{bad}")

    counts_long["population_id"] = counts_long["population_name"].map(pop_map)

    # shove them into the DB, and if it's already there, overwrite
    conn.executemany(
        "INSERT INTO cell_counts(sample_id, population_id, count) VALUES (?, ?, ?) "
        "ON CONFLICT(sample_id, population_id) DO UPDATE SET count=excluded.count",
        counts_long[["sample", "population_id", "count"]].itertuples(index=False, name=None),
    )
    conn.commit()

def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        init_db(conn)
        load_data_from_csv_to_db(DATA_PATH, conn)
        
        n_projects = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
        n_subjects = conn.execute("SELECT COUNT(*) FROM subjects").fetchone()[0]
        n_samples = conn.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
        n_counts = conn.execute("SELECT COUNT(*) FROM cell_counts").fetchone()[0]

        print("Loaded DB:", DB_PATH)
        print("  projects:   ", n_projects)
        print("  subjects:   ", n_subjects)
        print("  samples:    ", n_samples)
        print("  cell_counts:", n_counts)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
