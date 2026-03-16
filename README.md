# SimpleDB Example

Example usage of **@zidnigz/fs-db**, a lightweight file-based database for Node.js that supports SQL-like query syntax.

This package allows operations such as **INSERT, SELECT, UPDATE, aggregation, filtering, sorting, and pagination** without requiring a database server like MySQL or PostgreSQL.

---

# Installation

Install the package using **npm**:

```bash
npm install @zidnigz/fs-db
```

---

# Database Initialization

Import the package and create a database instance.

```javascript
const SimpleDB = require('@zidnigz/fs-db');
const db = new SimpleDB('./my_database');
```

The `./my_database` parameter specifies the folder where the database files are stored. If the folder does not exist, it will be created automatically.

---

# Usage Examples

## 1. Multiple Insert

Insert multiple records into the `users` table.

```javascript
db.query(
  "INSERT INTO users (name, age, role) VALUES ('Alice', 25, 'admin'), ('Bob', 19, 'user'), ('Charlie', 30, 'user')"
);
```

---

## 2. Complex Select

Query with conditions, column aliases, sorting, and pagination.

```javascript
const users = db.query(
  "SELECT name AS nama, age AS umur FROM users WHERE age >= 20 OR role = 'admin' ORDER BY age DESC LIMIT 2 OFFSET 0"
);

console.log(users);
```

Query features used:

* `SELECT`
* `AS` (column alias)
* `WHERE`
* `OR`
* `ORDER BY`
* `LIMIT`
* `OFFSET`

---

## 3. Data Aggregation

Calculate the total number of users and the average age.

```javascript
const stats = db.query(
  "SELECT COUNT(*) AS total_user, AVG(age) AS rata_umur FROM users WHERE role != 'admin'"
);

console.log(stats);
```

Available aggregation functions:

* `COUNT()`
* `AVG()`

---

## 4. Conditional Update

Update records based on specific conditions.

```javascript
db.query(
  "UPDATE users SET role = 'superadmin' WHERE name LIKE '%Ali%' AND age > 20"
);
```

Query features used:

* `UPDATE`
* `SET`
* `WHERE`
* `LIKE`
* `AND`

---

# Example Output

```javascript
[
  { nama: 'Charlie', umur: 30 },
  { nama: 'Alice', umur: 25 }
]

{
  total_user: 2,
  rata_umur: 24.5
}
```

---

# Main Features

* File system based database
* SQL-like query syntax
* Supports common database operations
* No database server required
* Suitable for small projects, bots, or prototypes

---

# Full Example

```javascript
const SimpleDB = require('@zidnigz/fs-db');
const db = new SimpleDB('./my_database');

// Insert multiple records
db.query("INSERT INTO users (name, age, role) VALUES ('Alice', 25, 'admin'), ('Bob', 19, 'user'), ('Charlie', 30, 'user')");

// Select data
const users = db.query("SELECT name AS nama, age AS umur FROM users WHERE age >= 20 OR role = 'admin' ORDER BY age DESC LIMIT 2 OFFSET 0");
console.log(users);

// Aggregation
const stats = db.query("SELECT COUNT(*) AS total_user, AVG(age) AS rata_umur FROM users WHERE role != 'admin'");
console.log(stats);

// Update records
db.query("UPDATE users SET role = 'superadmin' WHERE name LIKE '%Ali%' AND age > 20");
```

---

# License

MIT License
