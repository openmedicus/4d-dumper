<?php

/**
 * @package 4d-dumper
 * @link    http://github.com/PhenX/4d-dumper
 * @author  Fabien Ménager <fabien.menager@gmail.com>
 * @license http://www.gnu.org/copyleft/lesser.html GNU Lesser General Public License
 *
 * @link http://sources.4d.com/trac/4d_pdo4d
 * @link http://sources.4d.com/trac/4d_pdo4d/raw-attachment/wiki/WikiStart/SQL%20networking%20protocol.doc
 */

// CLI or die
PHP_SAPI === "cli" or die;

$options = array(
  "debug"    => false,
  "host"     => "127.0.0.1",
  "port"     => 19812,
  "username" => "administrateur",
  "password" => null,
  "output"   => ".",
  "schemaout" => null,
  "limit"    => 10000000,
  "tables"   => null,
);

for ($i = 1; $i < $argc; $i++) {
  switch ($argv[$i]) {
    case "--debug":
      $options["debug"] = true;
      break;

    case "--host":
    case "--port":
    case "--username":
    case "--password":
    case "--output":
    case "--schemaout":
    case "--limit":
    case "--tables":
      $options[substr($argv[$i], 2)] = $argv[++$i];
      break;
    default:
      // Ignore
  }
}

require_once __DIR__."/src/FourDClient.php";

$t = microtime(true);

$client = new FourDClient($options["host"], $options["port"]);

$client->login($options["username"], $options["password"]);

if (empty($options["tables"])) {
  $stmt = $client->execStatement("SELECT * FROM _USER_TABLES;");
  $list_tables = array();
  while ($row = $stmt->fetchRow()) {
    $_table_name = $row["TABLE_NAME"];
    $list_tables[] = $_table_name;
  }
}
else {
  $list_tables = explode(",", $options["tables"]);
}

$limit = (int)$options["limit"];

$output = $options["output"];
if (!is_dir($output)) {
  mkdir($output);
}

$schemaout = $options["schemaout"];
if (!is_dir(dirname($schemaout))) {
  mkdir(dirname($schemaout));
}

$schema = null;
if ($schemaout) {
  $schema = fopen($schemaout, "w");
}

$allographs = array(
  "withdiacritics"    => "àáâãäåòóôõöøèéêëçìíîïùúûüÿñ",
  "withoutdiacritics" => "aaaaaaooooooeeeeciiiiuuuuyn",
);

foreach ($list_tables as $_table) {
  $_table_name = strtoupper($_table);
  if ($_table == "User") $_table = "Users";
  echo "## $_table ##\n";

  // List columns
  $stmt_cols = $client->execStatement("SELECT * FROM _USER_COLUMNS WHERE UPPER(TABLE_NAME) = '$_table_name';");
  //print_r($stmt_cols);
  $cols = $stmt_cols->fetchAll();
  //print_r($cols);
  $stmt_cols->close();

  // List indexes
  $stmt_indexes = $client->execStatement("SELECT * FROM _USER_INDEXES WHERE UPPER(TABLE_NAME) = '$_table_name';");
  $indexes = $stmt_indexes->fetchAll();
  $stmt_indexes->close();

  $stmt_index_columns = $client->execStatement("SELECT * FROM _USER_IND_COLUMNS WHERE UPPER(TABLE_NAME) = '$_table_name';");
  $index_columns = $stmt_index_columns->fetchAll();
  $stmt_index_columns->close();

  $index_struct = array();
  foreach ($indexes as $_index) {
    $_id = $_index["INDEX_ID"];
    $index_struct[] = array(
      "columns" => array_filter(
        $index_columns,
        function ($v) use ($_id) {
          return $v["INDEX_ID"] === $_id;
        }
      ),
      "type"   => $_index["INDEX_TYPE"],
      "name"   => $_index["INDEX_NAME"],
      "unique" => $_index["UNIQUENESS"],
    );
  }

  $query = "DROP TABLE IF EXISTS $_table;\n";
  $query .= "CREATE TABLE $_table";
  $query_columns = array();
  $column_names = array();
  foreach ($cols as $_col) {
    $_name = str_replace($allographs["withdiacritics"], $allographs["withoutdiacritics"], $_col['COLUMN_NAME']);
    $_type = $_col["DATA_TYPE"];
    $_length = $_col["DATA_LENGTH"];
    $_nullable = $_col["NULLABLE"];

    if ($_name == "Default") $_name = "Default1";
    if ($_name == "default") $_name = "default1";
    if ($_name == "Check") $_name = "Check1";

    $column_names[] = $_name;

    /**
     * Field type in 4D   DATA_TYPE
     * ------------------------------
     * Boolean            1
     * Integer            3
     * Long Integer       4
     * Integer 64 Bits    5
     * Real               6
     * Float              7
     * Date               8
     * Time               9
     * Alpha              10 *
     * Text               10 *
     * Picture            12
     * UUID		  13
     * CLOB               14
     * BLOB               18
     */
    switch ($_type) {
      case 1:
        $_sql_type = "BOOL";
        break;
      case 3:
        $_sql_type = "SMALLINT";
        break;
      case 4:
        $_sql_type = "INT";
        break;
      case 5:
        $_sql_type = "BIGINT";
        break;
      case 6:
        $_sql_type = "DOUBLE PRECISION";
        break;
      case 7:
        $_sql_type = "REAL";
        break;
      case 8:
        $_sql_type = "DATE";
        break;
      case 9:
        $_sql_type = "TIME";
        break;
      case 10:
        if ($_length == 0 || $_length > 65535) {
          $_sql_type = "TEXT";
        }
        elseif ($_length > 255) {
          $_sql_type = "TEXT";
        }
        else {
          $_sql_type = "VARCHAR($_length)";
        }
        break;
      case 12:
      case 14:
      case 18:
        $_sql_type = "BYTEA";
        break;
      case 13:
        $_sql_type = "UUID";
        break;

      default:
        throw new Exception("Unknown data type $_type");
    }

    $query_columns[] = "$_name $_sql_type";
  }

  // Indexes
  $index_statements = array();
  foreach ($index_struct as $_struct) {
    $_columns = array();
    foreach ($_struct["columns"] as $_col) {
      $_name = str_replace($allographs["withdiacritics"], $allographs["withoutdiacritics"], $_col['COLUMN_NAME']);
      if ($_name == "Default") $_name = "Default1";
      if ($_name == "default") $_name = "default1";
      if ($_name == "Check") $_name = "Check1";
      $_columns[] = "$_name";
    }

    $_type = "INDEX";
    if ($_struct["unique"]) {
      $index_statements[] = "CREATE UNIQUE INDEX " . $_table . "_" . implode(', ', $_columns) . "_idx ON $_table (".implode(', ', $_columns).");";
    }
    else
    {
      $index_statements[] = "CREATE INDEX " . $_table . "_" . implode(', ', $_columns) . "_idx ON $_table (".implode(', ', $_columns).");";
    }
  }

  /*if (count($query_columns)) {
    $query .= " (\n  ".implode(",\n  ", $query_columns)."\n);\n";

    if (count($index_statements)) {
      $query .= implode("\n", $index_statements);
    }

    $query .= "\n\COPY $_table (" . implode(', ', $column_names) . ") FROM '$output/$_table.csv' DELIMITER ',' CSV HEADER;";

    if ($schema) {
      fwrite($schema, "-- $_table --\n$query\n\n");
    }
    else {
      file_put_contents("$output/$_table.sql", $query);
    }
  }*/

  $stmt = $client->execStatement("SELECT * FROM $_table_name LIMIT $limit;");

  $csv = fopen("$output/$_table.csv", "w");

  $_columns = array_map(
    function ($v) use ($allographs) {
      return str_replace($allographs["withdiacritics"], $allographs["withoutdiacritics"], $v);
    },
    $stmt->getColumns()
  );

  $s = fputcsv($csv, $stmt->getColumns());

  $n = 0;
  while ($row = $stmt->fetchRow()) {
    $row = array_map(
      function ($v) {
        $v = str_replace('\\"', '"', $v);
        $v = str_replace("\r", '', $v);
        $v = str_replace("\0", '', $v);
        $v = str_replace("\x01", '', $v);
        return str_replace('\\', '\\\\', $v);
      },
      $row
    );

    $s += fputcsv($csv, $row);

    if ($n % 500 === 0) {
      echo sprintf(" -> Rows: %d \tFile size: %s\r", $n, number_format($s, 0, ".", " "));
    }

    $n++;
  }

  fclose($csv);

  if ($n <= 0)
  {
	unlink("$output/$_table.csv");
  }

  echo sprintf(" -> %d rows exported \tFile size: %s\r", $n, number_format($s, 0, ".", " "));

  echo "\n";

  if ($n > 0 && count($query_columns)) {
    $query .= " (\n  ".implode(",\n  ", $query_columns)."\n);\n";

    if (count($index_statements)) {
      $query .= implode("\n", $index_statements);
    }

    $query .= "\n\COPY $_table (" . implode(', ', $column_names) . ") FROM '$output/$_table.csv' DELIMITER ',' CSV HEADER;";

    if ($schema) {
      fwrite($schema, "-- $_table --\n$query\n\n");
    }
    else {
      file_put_contents("$output/$_table.sql", $query);
    }
  }

  $stmt->close();
}

echo sprintf("Done in %.3f ms\n", (microtime(true) - $t) * 1000);
