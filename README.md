# SimpleDB Example

Contoh penggunaan **@zidnigz/fs-db**, sebuah database ringan berbasis file untuk Node.js yang menggunakan sintaks query mirip SQL.

Package ini memungkinkan kamu melakukan operasi **INSERT, SELECT, UPDATE, agregasi, filtering, sorting, dan pagination** tanpa perlu database server seperti MySQL atau PostgreSQL.

---

# Instalasi

Install package menggunakan **npm**:

```bash
npm install @zidnigz/fs-db
```

---

# Inisialisasi Database

Import package dan buat instance database.

```javascript
const SimpleDB = require('@zidnigz/fs-db');
const db = new SimpleDB('./my_database');
```

Parameter `./my_database` adalah folder tempat database disimpan. Jika folder belum ada, biasanya akan dibuat otomatis.

---

# Contoh Penggunaan

## 1. Multiple Insert

Menambahkan beberapa data sekaligus ke tabel `users`.

```javascript
db.query(
  "INSERT INTO users (name, age, role) VALUES ('Alice', 25, 'admin'), ('Bob', 19, 'user'), ('Charlie', 30, 'user')"
);
```

---

## 2. Complex Select

Query dengan kondisi, alias kolom, pengurutan, dan pembatasan jumlah data.

```javascript
const users = db.query(
  "SELECT name AS nama, age AS umur FROM users WHERE age >= 20 OR role = 'admin' ORDER BY age DESC LIMIT 2 OFFSET 0"
);

console.log(users);
```

Fitur yang digunakan pada query ini:

* `SELECT`
* `AS` (alias kolom)
* `WHERE`
* `OR`
* `ORDER BY`
* `LIMIT`
* `OFFSET`

---

## 3. Agregasi Data

Menghitung jumlah user dan rata-rata umur.

```javascript
const stats = db.query(
  "SELECT COUNT(*) AS total_user, AVG(age) AS rata_umur FROM users WHERE role != 'admin'"
);

console.log(stats);
```

Fungsi agregasi yang tersedia:

* `COUNT()`
* `AVG()`

---

## 4. Update Data dengan Kondisi Kompleks

Memperbarui data berdasarkan kondisi tertentu.

```javascript
db.query(
  "UPDATE users SET role = 'superadmin' WHERE name LIKE '%Ali%' AND age > 20"
);
```

Fitur yang digunakan:

* `UPDATE`
* `SET`
* `WHERE`
* `LIKE`
* `AND`

---

# Contoh Output

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

# Fitur Utama

* Database berbasis **file system**
* Query menggunakan **SQL-like syntax**
* Mendukung operasi dasar database
* Tidak membutuhkan server database
* Mudah digunakan untuk **project kecil, bot, atau prototype**

---

# Contoh Penggunaan Lengkap

```javascript
const SimpleDB = require('@zidnigz/fs-db');
const db = new SimpleDB('./my_database');

// Insert multiple data
db.query("INSERT INTO users (name, age, role) VALUES ('Alice', 25, 'admin'), ('Bob', 19, 'user'), ('Charlie', 30, 'user')");

// Select data
const users = db.query("SELECT name AS nama, age AS umur FROM users WHERE age >= 20 OR role = 'admin' ORDER BY age DESC LIMIT 2 OFFSET 0");
console.log(users);

// Aggregation
const stats = db.query("SELECT COUNT(*) AS total_user, AVG(age) AS rata_umur FROM users WHERE role != 'admin'");
console.log(stats);

// Update data
db.query("UPDATE users SET role = 'superadmin' WHERE name LIKE '%Ali%' AND age > 20");
```

---

# License

MIT License
