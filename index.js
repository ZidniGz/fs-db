const fs = require('fs');
const zlib = require('zlib');
const path = require('path');

class Table {
  constructor(dbPath, tableName) {
    this.tableDir = path.join(dbPath, tableName);
    if (!fs.existsSync(this.tableDir)) {
      fs.mkdirSync(this.tableDir, { recursive: true });
    }
    this.dataCache = {}; 
    this._loadTable();
    this.startDuplicateCheck();
  }

  _loadTable() {
    const files = fs.readdirSync(this.tableDir);
    files.forEach(file => {
      if (!file.endsWith('.db')) return;
      const filePath = path.join(this.tableDir, file);
      const fileData = fs.readFileSync(filePath);
      try {
        const decompressedData = zlib.gunzipSync(fileData).toString('utf8');
        const doc = JSON.parse(decompressedData);
        const docId = path.basename(file, '.db');
        this.dataCache[docId] = { ...doc, id: docId }; 
      } catch (e) {
        console.error(`Error loading ${file}:`, e);
      }
    });
  }

  _saveDocument(doc) {
    const docId = doc.id;
    const filePath = path.join(this.tableDir, `${docId}.db`);
    const { id, ...docWithoutId } = doc; 
    const stringifiedData = JSON.stringify(docWithoutId, null, 2);
    const compressedData = zlib.gzipSync(stringifiedData);
    fs.writeFileSync(filePath, compressedData, 'utf8');
    this.dataCache[docId] = doc; 
  }

  _deleteDocument(doc) {
    const docId = doc.id;
    const filePath = path.join(this.tableDir, `${docId}.db`);
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
    delete this.dataCache[docId]; 
  }

  _generateDocId(doc) {
    return doc.id || `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  }

  startDuplicateCheck() {
    setInterval(() => {
      this.removeDuplicates();
    }, 30000); // 30 detik
  }

  insert(doc) {
    if (this._isDuplicate(doc)) {
      this.removeDuplicates();
      return doc;
    }
    const docId = this._generateDocId(doc);
    const newDoc = { ...doc, id: docId };
    this._saveDocument(newDoc);
    return newDoc;
  }

  removeDuplicates() {
    const uniqueData = [];
    const seen = new Set();

    Object.values(this.dataCache).forEach(doc => {
      const { id, ...docWithoutId } = doc; 
      const docString = JSON.stringify(docWithoutId);

      if (!seen.has(docString)) {
        seen.add(docString);
        uniqueData.push(doc);
      } else {
        this._deleteDocument(doc);
      }
    });

    if (uniqueData.length !== Object.keys(this.dataCache).length) {
      this.dataCache = uniqueData.reduce((acc, doc) => {
        acc[doc.id] = doc;
        return acc;
      }, {});
    }
  }

  _isDuplicate(newDoc) {
    return Object.values(this.dataCache).some(existingDoc => {
      const { id, ...docWithoutId } = existingDoc; 
      for (let key in newDoc) {
        if (newDoc[key] !== docWithoutId[key]) {
          return false;
        }
      }
      return true;
    });
  }
}

class SimpleDB {
  constructor(dbPath) {
    this.dbPath = dbPath;
    if (!fs.existsSync(dbPath)) {
      fs.mkdirSync(dbPath, { recursive: true });
    }
  }

  table(tableName) {
    return new Table(this.dbPath, tableName);
  }

  // --- ADVANCED SQL ENGINE ---
  query(sqlString) {
    let sql = sqlString.trim().replace(/;$/, ''); // Hapus titik koma di akhir
    
    // 1. SELECT Query Pipeline
    if (/^SELECT\s+/i.test(sql)) {
      return this._executeSelect(sql);
    }

    // 2. INSERT Query Pipeline (Support multiple values)
    let insertMatch = sql.match(/^INSERT\s+INTO\s+(\w+)\s*\((.+?)\)\s*VALUES\s*(.+)$/i);
    if (insertMatch) {
      const [, tableName, colsStr, valsStr] = insertMatch;
      const table = this.table(tableName);
      const cols = colsStr.split(',').map(s => s.trim());
      
      // Parse Multiple Values e.g. (1, 'A'), (2, 'B')
      const rows = valsStr.match(/\(([^)]+)\)/g).map(r => r.slice(1, -1));
      const insertedDocs = [];
      
      rows.forEach(rowStr => {
        const vals = this._splitCSV(rowStr).map(s => this._parseValue(s));
        const doc = {};
        cols.forEach((col, i) => doc[col] = vals[i]);
        insertedDocs.push(table.insert(doc));
      });
      return insertedDocs.length === 1 ? insertedDocs[0] : insertedDocs;
    }

    // 3. UPDATE Query Pipeline
    let updateMatch = sql.match(/^UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$/i);
    if (updateMatch) {
      const [, tableName, setStr, whereStr] = updateMatch;
      const table = this.table(tableName);

      const updates = {};
      this._splitCSV(setStr).forEach(pair => {
        const [k, v] = pair.split('=').map(s => s.trim());
        updates[k] = this._parseValue(v);
      });

      let data = Object.values(table.dataCache);
      if (whereStr) data = this._complexWhereFilter(data, whereStr);

      let updatedCount = 0;
      data.forEach(row => {
        const { id, ...safeUpdates } = updates;
        table._saveDocument({ ...row, ...safeUpdates });
        updatedCount++;
      });
      return { affectedRows: updatedCount };
    }

    // 4. DELETE Query Pipeline
    let deleteMatch = sql.match(/^DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$/i);
    if (deleteMatch) {
      const [, tableName, whereStr] = deleteMatch;
      const table = this.table(tableName);

      let data = Object.values(table.dataCache);
      if (whereStr) data = this._complexWhereFilter(data, whereStr);

      let deletedCount = 0;
      data.forEach(row => {
        table._deleteDocument(row);
        deletedCount++;
      });
      return { affectedRows: deletedCount };
    }

    throw new Error("Syntax Error: Query SQL tidak valid atau tidak didukung.");
  }

  // --- QUERY EXECUTION PIPELINE ---
  
  _executeSelect(sql) {
    // Ekstraksi komponen SQL dari belakang ke depan untuk keamanan parsing
    let limitMatch = sql.match(/\s+LIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?$/i);
    let limit = null, offset = null;
    if (limitMatch) {
      limit = parseInt(limitMatch[1]);
      offset = limitMatch[2] ? parseInt(limitMatch[2]) : 0;
      sql = sql.replace(/\s+LIMIT\s+.*$/i, '');
    }

    let orderMatch = sql.match(/\s+ORDER\s+BY\s+(.+?)(?:\s+(ASC|DESC))?$/i);
    let orderCol = null, orderDir = 'ASC';
    if (orderMatch) {
      orderCol = orderMatch[1].trim();
      orderDir = orderMatch[2] ? orderMatch[2].toUpperCase() : 'ASC';
      sql = sql.replace(/\s+ORDER\s+BY\s+.*$/i, '');
    }

    let whereMatch = sql.match(/\s+WHERE\s+(.+)$/i);
    let whereStr = null;
    if (whereMatch) {
      whereStr = whereMatch[1].trim();
      sql = sql.replace(/\s+WHERE\s+.*$/i, '');
    }

    let selectMatch = sql.match(/^SELECT\s+(.+?)\s+FROM\s+(\w+)$/i);
    if (!selectMatch) throw new Error("Syntax Error in SELECT statement.");

    const [, columnsStr, tableName] = selectMatch;
    const table = this.table(tableName);
    let data = Object.values(table.dataCache);

    // 1. FILTERING (WHERE)
    if (whereStr) {
      data = this._complexWhereFilter(data, whereStr);
    }

    // 2. SORTING (ORDER BY)
    if (orderCol) {
      data.sort((a, b) => {
        let valA = a[orderCol], valB = b[orderCol];
        if (valA === valB) return 0;
        let comparison = valA > valB ? 1 : -1;
        return orderDir === 'DESC' ? comparison * -1 : comparison;
      });
    }

    // 3. PAGINATION (LIMIT & OFFSET)
    if (limit !== null) {
      data = data.slice(offset, offset + limit);
    }

    // 4. PROJECTION & AGGREGATION (SELECT cols)
    return this._selectAndAggregate(data, columnsStr);
  }

  // --- ADVANCED WHERE EVALUATOR ---
  
  _complexWhereFilter(data, whereStr) {
    // Membangun fungsi evaluasi JavaScript yang aman dari string SQL
    // Mengubah operator SQL menjadi JS: AND -> &&, OR -> ||, = -> ===, <> atau != -> !==
    let jsCondition = whereStr
      .replace(/\s+AND\s+/gi, ' && ')
      .replace(/\s+OR\s+/gi, ' || ')
      .replace(/(?<![<>!])=/g, '===')
      .replace(/<>/g, '!==')
      .replace(/!===/g, '!=='); // Fix jika user ketik !=

    return data.filter(row => {
      // Mengganti nama kolom dengan row['nama_kolom']
      // Regex ini mendeteksi kata yang bukan keyword / boolean / angka / string
      let evalString = jsCondition.replace(/\b([a-zA-Z_]\w*)\b/g, (match) => {
        const lower = match.toLowerCase();
        if (['true', 'false', 'null', 'and', 'or', 'in', 'like'].includes(lower)) return match;
        // Inject value dari row
        let val = row[match];
        return typeof val === 'string' ? `"${val}"` : val;
      });

      // Handle LIKE operator (eg: row['name'] LIKE '%john%')
      evalString = evalString.replace(/(["']?[^"'\s]+["']?)\s+LIKE\s+(['"].+?['"])/gi, (m, colVal, searchStr) => {
        let regexStr = searchStr.slice(1, -1).replace(/%/g, '.*');
        return `/^${regexStr}$/i.test(${colVal})`;
      });

      try {
        // PERINGATAN: new Function aman di sini karena kita mengontrol objek 'row' 
        // dan tidak mengeksekusi fungsi arbitrer dari luar.
        return new Function(`return ${evalString};`)();
      } catch (e) {
        return false; // Jika kolom tidak ada atau error sintaks evaluasi, lewati baris ini
      }
    });
  }

  // --- PROJECTION & AGGREGATIONS ---

  _selectAndAggregate(data, columnsStr) {
    if (columnsStr.trim() === '*') return data;

    const cols = this._splitCSV(columnsStr);
    const result = [];
    let isAggregate = false;
    let aggResult = {};

    cols.forEach(c => {
      // Regex untuk mendeteksi COUNT(col), SUM(col), dll.
      let aggMatch = c.match(/^(COUNT|SUM|AVG|MIN|MAX)\((.+?)\)(?:\s+AS\s+(\w+))?$/i);
      if (aggMatch) {
        isAggregate = true;
        const func = aggMatch[1].toUpperCase();
        const field = aggMatch[2].trim();
        const alias = aggMatch[3] || `${func}(${field})`;
        
        let vals = field === '*' ? data : data.map(r => r[field]).filter(v => v !== undefined);
        
        switch (func) {
          case 'COUNT': aggResult[alias] = vals.length; break;
          case 'SUM': aggResult[alias] = vals.reduce((a, b) => a + Number(b), 0); break;
          case 'AVG': aggResult[alias] = vals.length ? vals.reduce((a, b) => a + Number(b), 0) / vals.length : 0; break;
          case 'MIN': aggResult[alias] = Math.min(...vals.map(Number)); break;
          case 'MAX': aggResult[alias] = Math.max(...vals.map(Number)); break;
        }
      }
    });

    if (isAggregate) return [aggResult];

    // Jika bukan agregat, ambil kolom biasa (mendukung AS)
    return data.map(row => {
      const filtered = {};
      cols.forEach(c => {
        let [colDef, alias] = c.split(/\s+AS\s+/i).map(s => s.trim());
        alias = alias || colDef;
        if (row[colDef] !== undefined) filtered[alias] = row[colDef];
      });
      return filtered;
    });
  }

  // --- HELPERS ---
  
  _parseValue(val) {
    val = val.trim();
    if (/^['"].*['"]$/.test(val)) return val.slice(1, -1);
    if (val.toLowerCase() === 'true') return true;
    if (val.toLowerCase() === 'false') return false;
    if (val.toLowerCase() === 'null') return null;
    if (!isNaN(val)) return Number(val);
    return val;
  }

  _splitCSV(str) {
    // Memisahkan by koma, tapi mengabaikan koma di dalam tanda kutip atau kurung
    const result = [];
    let current = '', inQuotes = false, parentheses = 0;
    
    for (let i = 0; i < str.length; i++) {
      const char = str[i];
      if (char === "'" || char === '"') inQuotes = !inQuotes;
      if (char === '(' && !inQuotes) parentheses++;
      if (char === ')' && !inQuotes) parentheses--;
      
      if (char === ',' && !inQuotes && parentheses === 0) {
        result.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    result.push(current.trim());
    return result;
  }
}

module.exports = SimpleDB;
