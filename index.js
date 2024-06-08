const fs = require('fs');
const zlib = require('zlib');
const path = require('path');

class Collection {
  constructor(dbPath, collectionName) {
    this.collectionDir = path.join(dbPath, collectionName);
    if (!fs.existsSync(this.collectionDir)) {
      fs.mkdirSync(this.collectionDir);
    }
    this.dataCache = {}; // Cache for storing data in memory
    this._loadCollection();
    this.startDuplicateCheck();
  }

  _loadCollection() {
    const files = fs.readdirSync(this.collectionDir);
    files.forEach(file => {
      const filePath = path.join(this.collectionDir, file);
      const fileData = fs.readFileSync(filePath);
      const decompressedData = zlib.gunzipSync(fileData).toString('utf8');
      const doc = JSON.parse(decompressedData);
      const docId = path.basename(file, '.db');
      this.dataCache[docId] = { ...doc, id: docId }; // Store in cache with id
    });
  }

  _saveDocument(doc) {
    const docId = doc.id;
    const filePath = path.join(this.collectionDir, `${docId}.db`);
    const { id, ...docWithoutId } = doc; // Remove id from doc before saving
    const stringifiedData = JSON.stringify(docWithoutId, null, 2);
    const compressedData = zlib.gzipSync(stringifiedData);
    fs.writeFileSync(filePath, compressedData, 'utf8');
    this.dataCache[docId] = doc; // Update cache
  }

  _deleteDocument(doc) {
    const docId = doc.id;
    const filePath = path.join(this.collectionDir, `${docId}.db`);
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
    delete this.dataCache[docId]; // Remove from cache
  }

  _generateDocId(doc) {
    return doc.id || `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  }

  startDuplicateCheck() {
    setInterval(() => {
      this.removeDuplicates();
    }, 30000);
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

  find(query) {
    return Object.values(this.dataCache).filter(doc => this._matches(doc, query));
  }

  findOne(query) {
    return Object.values(this.dataCache).find(doc => this._matches(doc, query));
  }

  update(query, updateDoc) {
    let updated = false;
    Object.keys(this.dataCache).forEach(id => {
      const doc = this.dataCache[id];
      if (this._matches(doc, query)) {
        updated = true;
        const updatedDoc = { ...doc, ...updateDoc };
        this._saveDocument(updatedDoc);
      }
    });
    return updated;
  }

  remove(query) {
    let removed = false;
    Object.keys(this.dataCache).forEach(id => {
      const doc = this.dataCache[id];
      if (this._matches(doc, query)) {
        this._deleteDocument(doc);
        removed = true;
      }
    });
    return removed;
  }

  removeDuplicates() {
    const uniqueData = [];
    const seen = new Set();

    Object.values(this.dataCache).forEach(doc => {
      const { id, ...docWithoutId } = doc; // Exclude id from comparison
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
      const { id, ...docWithoutId } = existingDoc; // Exclude id from comparison
      for (let key in newDoc) {
        if (newDoc[key] !== docWithoutId[key]) {
          return false;
        }
      }
      return true;
    });
  }

  _matches(doc, query) {
    for (let key in query) {
      if (query[key] !== doc[key]) {
        return false;
      }
    }
    return true;
  }
}

class SimpleDB {
  constructor(dbPath) {
    this.dbPath = dbPath;
    if (!fs.existsSync(dbPath)) {
      fs.mkdirSync(dbPath);
    }
  }

  collection(collectionName) {
    return new Collection(this.dbPath, collectionName);
  }
}

module.exports = SimpleDB
