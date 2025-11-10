# DSCI551_Foundations_of_Data_Management

# 🧠 Talk2DB — Natural Language to SQL Query System

### *Query relational databases using plain English*

Talk2DB is a lightweight **rule-based natural language to SQL translator** developed as part of the *Foundations of Data Management* course at USC.
It enables users to interact with a database through conversational English queries without writing SQL, using a custom-built **template-matching and parsing system**.

---

## 🚀 Project Overview

The goal of Talk2DB is to make data retrieval more intuitive for non-technical users.
Instead of requiring knowledge of SQL syntax, users can type questions like:

> 💬 *“Show me all employees hired after 2020”*
> ⏩ Generates SQL:
>
> ```sql
> SELECT * FROM employees WHERE hire_year > 2020;
> ```

The system identifies keywords, entity names, and conditions in the user’s input, then matches them against a set of **predefined SQL templates** to build valid SQL queries.

---

## 🧩 How It Works

Talk2DB uses a **rule-based pipeline** consisting of:

1. **Query Preprocessing**

   * Tokenizes user input and removes stop words.
   * Identifies key components like tables, attributes, and filters.

2. **Intent Detection & Template Matching**

   * Matches parsed queries to predefined **SQL templates** (e.g., SELECT, COUNT, AVG, GROUP BY).
   * Uses keyword similarity and structural rules to find the best match.

3. **SQL Query Construction**

   * Dynamically fills template slots with detected entities and conditions.
   * Ensures syntactic correctness via basic grammar validation.

4. **Query Execution**

   * Executes the generated SQL on a **PostgreSQL** or **SQLite** database.
   * Displays query results in a user-friendly table using **Streamlit**.

---

## ⚙️ Tech Stack

| Component     | Tools / Libraries         |
| ------------- | ------------------------- |
| Language      | Python                    |
| Database      | PostgreSQL / SQLite       |
| Parsing       | NLTK, Regular Expressions |
| Interface     | Streamlit                 |
| Data Handling | Pandas, SQLAlchemy        |

---

## 📈 Example Queries

| Natural Language Input                             | Generated SQL                                                     |
| -------------------------------------------------- | ----------------------------------------------------------------- |
| “List all students enrolled in CSCI 585.”          | `SELECT * FROM students WHERE course = 'CSCI 585';`               |
| “Find the number of employees in each department.” | `SELECT department, COUNT(*) FROM employees GROUP BY department;` |
| “Show the average salary of analysts.”             | `SELECT AVG(salary) FROM employees WHERE role = 'Analyst';`       |

---

## 🧠 Key Features

* Handles **SELECT**, **AGGREGATE**, and **FILTER** queries.
* Supports **JOIN** and **GROUP BY** for multi-table queries.
* Modular design for easy addition of new SQL templates.
* Interactive **Streamlit UI** for accessible querying.

---

## 📚 Key Learnings

* Gained deep understanding of **SQL semantics**, **query parsing**, and **language structures**.
* Designed a scalable architecture for rule-based natural language interpretation.
* Strengthened Python proficiency in **regex-based text parsing** and **database connectivity**.

---

## 🔮 Future Work

While Talk2DB currently relies on deterministic, rule-based mappings, future versions could:

* Integrate **AI or LLM-based intent detection** for more flexible, context-aware query generation.
* Support **schema-agnostic** query handling for dynamic database structures.
* Add **multi-turn conversation support**, allowing follow-up questions and refinements.
* Include **visual SQL debugging**, so users can see how their natural query maps to SQL logic.

These extensions would enhance both the system’s usability and adaptability to real-world databases.

---

## 🧑‍💻 How to Run

```bash
# Clone the repository
git clone https://github.com/yourusername/talk2db.git
cd talk2db

# Install dependencies
pip install -r requirements.txt

# Run the Streamlit app
streamlit run app.py
```

---
