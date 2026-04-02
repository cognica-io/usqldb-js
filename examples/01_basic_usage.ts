// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// Basic USQLEngine usage: DDL, DML, and queries.
//
// Demonstrates creating tables with various column types and constraints,
// inserting/updating/deleting rows, and running SELECT queries.

import { USQLEngine } from "../src/core/engine.js";

async function main(): Promise<void> {
  const engine = new USQLEngine();

  // ---- DDL: Create tables with constraints ----------------------------

  await engine.sql(`
    CREATE TABLE departments (
      id    SERIAL PRIMARY KEY,
      name  TEXT NOT NULL UNIQUE,
      budget NUMERIC
    )
  `);

  await engine.sql(`
    CREATE TABLE employees (
      id         SERIAL PRIMARY KEY,
      name       TEXT NOT NULL,
      email      VARCHAR(255) UNIQUE,
      department_id INTEGER REFERENCES departments(id),
      salary     REAL,
      active     BOOLEAN DEFAULT TRUE
    )
  `);

  console.log("Tables created.\n");

  // ---- DML: Insert rows -----------------------------------------------

  await engine.sql("INSERT INTO departments (name, budget) VALUES ('Engineering', 500000)");
  await engine.sql("INSERT INTO departments (name, budget) VALUES ('Marketing', 200000)");
  await engine.sql("INSERT INTO departments (name, budget) VALUES ('Sales', 300000)");

  await engine.sql(
    "INSERT INTO employees (name, email, department_id, salary) "
    + "VALUES ('Alice', 'alice@example.com', 1, 120000)",
  );
  await engine.sql(
    "INSERT INTO employees (name, email, department_id, salary) "
    + "VALUES ('Bob', 'bob@example.com', 1, 110000)",
  );
  await engine.sql(
    "INSERT INTO employees (name, email, department_id, salary) "
    + "VALUES ('Charlie', 'charlie@example.com', 2, 95000)",
  );
  await engine.sql(
    "INSERT INTO employees (name, email, department_id, salary, active) "
    + "VALUES ('Diana', 'diana@example.com', 3, 105000, FALSE)",
  );

  console.log("Data inserted.\n");

  // ---- SELECT: Basic queries ------------------------------------------

  console.log("=== All departments ===");
  const depts = await engine.sql("SELECT id, name, budget FROM departments ORDER BY id");
  for (const row of depts!.rows) {
    const id = String(row["id"]).padStart(2);
    const name = String(row["name"]).padEnd(15);
    console.log(`  ${id}  ${name} budget=${row["budget"]}`);
  }

  console.log("\n=== Active employees with salary > 100000 ===");
  const emps = await engine.sql(
    "SELECT name, email, salary "
    + "FROM employees "
    + "WHERE active = TRUE AND salary > 100000 "
    + "ORDER BY salary DESC",
  );
  for (const row of emps!.rows) {
    const name = String(row["name"]).padEnd(10);
    const email = String(row["email"]).padEnd(25);
    const salary = Number(row["salary"]).toLocaleString("en-US", {
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    });
    console.log(`  ${name} ${email} $${salary.padStart(10)}`);
  }

  // ---- UPDATE ---------------------------------------------------------

  await engine.sql("UPDATE employees SET salary = 125000 WHERE name = 'Alice'");
  const updated = await engine.sql("SELECT name, salary FROM employees WHERE name = 'Alice'");
  const aliceSalary = Number(updated!.rows[0]["salary"]).toLocaleString("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
  console.log(`\nAlice's updated salary: $${aliceSalary}`);

  // ---- DELETE ---------------------------------------------------------

  await engine.sql("DELETE FROM employees WHERE active = FALSE");
  const remaining = await engine.sql("SELECT COUNT(*) AS cnt FROM employees");
  console.log(`Active employees remaining: ${remaining!.rows[0]["cnt"]}`);

  // ---- Aggregation ----------------------------------------------------

  console.log("\n=== Salary statistics ===");
  const stats = await engine.sql(`
    SELECT
      COUNT(*) AS headcount,
      AVG(salary) AS avg_salary,
      MIN(salary) AS min_salary,
      MAX(salary) AS max_salary
    FROM employees
  `);
  const srow = stats!.rows[0];
  console.log(`  Headcount: ${srow["headcount"]}`);
  const avgSalary = Number(srow["avg_salary"]).toLocaleString("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
  const minSalary = Number(srow["min_salary"]).toLocaleString("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
  const maxSalary = Number(srow["max_salary"]).toLocaleString("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
  console.log(`  Avg salary: $${avgSalary.padStart(10)}`);
  console.log(`  Min salary: $${minSalary.padStart(10)}`);
  console.log(`  Max salary: $${maxSalary.padStart(10)}`);
}

main();
