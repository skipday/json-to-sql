const jsonfile = require('jsonfile');
const fs = require('fs');

/**
 * ===========================
 * Command line interface
 * ===========================
 */

// Extract command line arguments
const input = process.argv.splice(2);
const [jsonFilename, sqlFilename] = input;
parseIfNotExist();
/**
 * ===========================
 * Implementation
 * ===========================
 */

Number.prototype.countDecimals = function () {

  if (Math.floor(this.valueOf()) === this.valueOf()) return 0;

  var str = this.toString();
  if (str.indexOf(".") !== -1 && str.indexOf("-") !== -1) {
      return str.split("-")[1] || 0;
  } else if (str.indexOf(".") !== -1) {
      return str.split(".")[1].length || 0;
  }
  return str.split("-")[1] || 0;
}

function parseIfNotExist(){
  fs.open(sqlFilename, 'r', function (fileNotExist, _) {
    converter(input);
  })
}

function converter(input) {

  // exit if json or sql files are not specified
  if (!jsonFilename || !sqlFilename) return 'Error';

  const tables = [];
  var columns = [];
  var values = [];
  const valueInserts = [];
  const createTables = [];
  const timestampKeys = ["inserted_at", "updated_at"]

  // use jsonfile module to read json file
  jsonfile.readFile(jsonFilename, (err, data) => {
    if (err) return console.error(err);

    const source = data;
    fetchTables(source);
    for (let i = 0; i < tables.length; i++) {
      const tableItem = source[tables[i]];
      if (Array.isArray(tableItem)) {
        parseArray(tableItem, i);
      }
      else if (typeof (tableItem) == "object") {
        parseObject(tableItem, i);
      }
    }
    const creates = toSql(createTables);
    const inserts = toSql(valueInserts);
    const combinedSql = creates.concat(`\n` + inserts)

    writeOutput(combinedSql)
  });

  function fetchTables(source) {
    for (var i in source) {
      tables.push(i);
    }
  }

  function insertTableHeader(index){
    const unsortedColumns = [...columns, ...timestampKeys.filter(key => !columns.includes(key))].map((col, i) => parseColumnInfo(col, i))
    const sortedColumns = unsortedColumns.sort((a,b) => 
      a.includes('inserted_at') && (!b.includes('inserted_at') && !b.includes('updated_at')) ? 1 :
      a.includes('updated_at') && !b.includes('updated_at') ? 1 : -1
    )
    createTables.push(`CREATE TABLE IF NOT EXISTS ${tables[index]} (${sortedColumns})`)
  }

  function parseArray(tableItem, index) {
    for (var i = 0; i < tableItem.length; i++) {
      convertObject(tableItem[i]);
      if (i == 1) insertTableHeader(index)
      const toAddKeys = timestampKeys.filter(key => !columns.includes(key))
      const allValueKeysSorted = [i, ...values.sort((a,b) => a.includes('TIMESTAMP') && !b.includes('TIMESTAMP') ? 1 : -1), ...toAddKeys.map(_key => "NOW()")];
      const allColumnKeysSorted = ['id', ...columns.sort((a,b) => a.includes('inserted_at') && !b.includes('inserted_at') ? 1 : -1), ...toAddKeys]
      let query = `INSERT INTO ${tables[index]} (${allColumnKeysSorted}) VALUES (${allValueKeysSorted})`
      query = query.replace(/\"/g, "'");
      valueInserts.push(query)
    }
  }

  // function parseObject(tableItem, index) {
  //   convertObject(tableItem)
  //   columns.forEach((col, i) => parseColumnInfo(col, i))
  //   createTables.push(`CREATE TABLE IF NOT EXISTS ${tables[index]} (${columnInfo})`)
  //   const toAddKeys = timestampKeys.filter(key => !columns.includes(key))
  //   const allKeys = [i, ...values, ...toAddKeys]
  //   console.log(allKeys)
  //   let query = `INSERT INTO ${tables[index]} (id,${columns}, ${toAddKeys}) VALUES (${i}, ${values},${toAddKeys.map(_key => "NOW()")})`
  //   query = query.replace(/\"/g, "'");
  //   valueInserts.push(query)
  // }

  function convertObject(item) {
    columns = [];
    values = [];
    for (var i in item) {
      columns.push(i);
      let value = item[i]
      if(typeof value === 'object') {
        value = 
          value.every(val => typeof val === 'number' && val.countDecimals() === 7) ? "point(" + value + ")" :
          "'{" + value.reduce((acc, val, index) => acc + val + (index === value.length - 1 ? "" : ","), "") + "}'"
      }
      else if (typeof value === 'string') value = "\"" + value + "\"";
      else if (value > 999999999) value = "TIMESTAMP 'epoch' + " + value + " * INTERVAL '1 second'"
      else if (value == null) value = "\"\""

      values.push(value);
    }
  }

  function parseColumnInfo(column, i) {
    let columnTypes = ""
    if(["inserted_at", "updated_at"].includes(column)) columnTypes = "TIMESTAMP"
    else if(typeof (values[i]) == "string") {
      if (values[i].includes("TIMESTAMP")) columnTypes = "TIMESTAMP"
      else if (values[i].includes("point")) columnTypes = "POINT"
      else columnTypes = "TEXT"
    }
    else if (typeof (values[i]) == "number") columnTypes = "INTERGER"
    return `${column} ${columnTypes}`
  }
 
  function toSql(queries) {
    return queries.join(`;\n`) + ';';
  }

  function writeOutput(combinedSql) {
    fs.writeFile(sqlFilename, combinedSql, (err2) => {
      if (err2) return console.error(err2);
      console.log('Done');
    });
  }
}