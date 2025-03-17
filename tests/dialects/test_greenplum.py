import sqlglot
from sqlglot import exp, parse_one
from sqlglot.dialects.greenplum import (
    DistributedByProperty, 
    DistributedRandomlyProperty,
    ExternalProperty,
    WritableProperty,
    ReadableProperty,
    LocationProperty,
    FormatProperty
)
from tests.dialects.test_dialect import Validator


class TestGreenplum(Validator):
    """
    Test for Greenplum dialect functionality.
    """
    maxDiff = None
    dialect = "greenplum"

    def test_greenplum_inherits_postgres(self):
        """Test that Greenplum inherits PostgreSQL functionality correctly."""
        sql = "SELECT * FROM table WHERE col1 = 'value'"
        self.assertEqual(
            sqlglot.transpile(sql, read="postgres", write="greenplum")[0],
            sql
        )
        
        # Test PostgreSQL-specific functionality is available in Greenplum
        self.validate_identity("x ? y", "x ? y")
        self.validate_identity("SHA384(x)")
        self.validate_identity("1.x", "1. AS x")
        self.validate_identity("|/ x", "SQRT(x)")
        self.validate_identity("||/ x", "CBRT(x)")

    def test_distributed_by(self):
        """Test Greenplum's DISTRIBUTED BY clause."""
        sql = "CREATE TABLE my_table (id INT, name TEXT) DISTRIBUTED BY (id)"
        
        # Verify it transpiles correctly
        self.assertEqual(
            sqlglot.transpile(sql, read="greenplum", write="greenplum")[0],
            sql
        )
        
        # Transpile from PostgreSQL to Greenplum with DISTRIBUTED BY added
        postgres_sql = "CREATE TABLE my_table (id INT, name TEXT)"
        transformed_sql = "CREATE TABLE my_table (id INT, name TEXT) DISTRIBUTED BY (id)"
        
        # Parse the PostgreSQL query and add the DISTRIBUTED BY property
        create_table = parse_one(postgres_sql, dialect="postgres") 
        
        # Add DISTRIBUTED BY property
        if not create_table.args.get("properties"):
            create_table.set("properties", exp.Properties(expressions=[]))
            
        create_table.args["properties"].append(
            "expressions", 
            DistributedByProperty(expressions=[exp.column("id")])
        )
        
        # Generate the Greenplum SQL
        generated_sql = create_table.sql(dialect="greenplum")
        self.assertEqual(generated_sql, transformed_sql)

    def test_distributed_randomly(self):
        """Test Greenplum's DISTRIBUTED RANDOMLY clause."""
        sql = "CREATE TABLE my_table (id INT, name TEXT) DISTRIBUTED RANDOMLY"
        
        # Verify it transpiles correctly
        self.assertEqual(
            sqlglot.transpile(sql, read="greenplum", write="greenplum")[0],
            sql
        )
        
        # Transpile from PostgreSQL to Greenplum with DISTRIBUTED RANDOMLY added
        postgres_sql = "CREATE TABLE my_table (id INT, name TEXT)"
        transformed_sql = "CREATE TABLE my_table (id INT, name TEXT) DISTRIBUTED RANDOMLY"
        
        # Parse the PostgreSQL query and add the DISTRIBUTED RANDOMLY property
        create_table = parse_one(postgres_sql, dialect="postgres") 
        
        # Add DISTRIBUTED RANDOMLY property
        if not create_table.args.get("properties"):
            create_table.set("properties", exp.Properties(expressions=[]))
            
        create_table.args["properties"].append(
            "expressions", 
            DistributedRandomlyProperty()
        )
        
        # Generate the Greenplum SQL
        generated_sql = create_table.sql(dialect="greenplum")
        self.assertEqual(generated_sql, transformed_sql)
        
    def test_external_table(self):
        """Test Greenplum's EXTERNAL TABLE clause."""
        # Test basic external table creation
        sql = "CREATE EXTERNAL TABLE ext_table (id INT, name TEXT) LOCATION ('file://host/path/file.csv') FORMAT 'CSV'"
        
        # Verify it transpiles correctly
        self.assertEqual(
            sqlglot.transpile(sql, read="greenplum", write="greenplum")[0],
            sql
        )
        
        # Test with multiple locations
        sql_multi_loc = "CREATE EXTERNAL TABLE ext_table (id INT, name TEXT) LOCATION ('file://host1/path/file1.csv', 'file://host2/path/file2.csv') FORMAT 'CSV'"
        self.assertEqual(
            sqlglot.transpile(sql_multi_loc, read="greenplum", write="greenplum")[0],
            sql_multi_loc
        )
        
        # Test with explicit READABLE keyword
        sql_readable = "CREATE READABLE EXTERNAL TABLE ext_table (id INT, name TEXT) LOCATION ('file://host/path/file.csv') FORMAT 'CSV'"
        self.assertEqual(
            sqlglot.transpile(sql_readable, read="greenplum", write="greenplum")[0],
            sql_readable
        )
        
        # Test programmatic creation
        create_table = parse_one("CREATE TABLE ext_table (id INT, name TEXT)", dialect="postgres")
        
        # Add EXTERNAL property
        if not create_table.args.get("properties"):
            create_table.set("properties", exp.Properties(expressions=[]))
            
        create_table.args["properties"].append(
            "expressions", 
            ExternalProperty()
        )
        
        # Add LOCATION property
        create_table.args["properties"].append(
            "expressions", 
            LocationProperty(this=exp.Array(expressions=[exp.Literal.string("file://host/path/file.csv")]))
        )
        
        # Add FORMAT property
        create_table.args["properties"].append(
            "expressions", 
            FormatProperty(this=exp.Literal.string("CSV"))
        )
        
        # Generate the Greenplum SQL
        generated_sql = create_table.sql(dialect="greenplum")
        self.assertEqual(
            generated_sql, 
            "CREATE EXTERNAL TABLE ext_table (id INT, name TEXT) LOCATION ('file://host/path/file.csv') FORMAT 'CSV'"
        )
    
    def test_writable_external_table(self):
        """Test Greenplum's WRITABLE EXTERNAL TABLE clause."""
        # Test writable external table
        sql_writable = "CREATE WRITABLE EXTERNAL TABLE write_table (id INT, name TEXT) LOCATION ('gpfdist://outputhost:8081/export.csv') FORMAT 'CSV'"
        
        self.assertEqual(
            sqlglot.transpile(sql_writable, read="greenplum", write="greenplum")[0],
            sql_writable
        )
        
        # Test programmatic creation
        create_table = parse_one("CREATE TABLE write_table (id INT, name TEXT)", dialect="postgres")
        
        # Add WRITABLE and EXTERNAL properties
        if not create_table.args.get("properties"):
            create_table.set("properties", exp.Properties(expressions=[]))
            
        create_table.args["properties"].append(
            "expressions", 
            WritableProperty()
        )
        
        create_table.args["properties"].append(
            "expressions", 
            ExternalProperty()
        )
        
        # Add LOCATION property
        create_table.args["properties"].append(
            "expressions", 
            LocationProperty(this=exp.Array(expressions=[exp.Literal.string("gpfdist://outputhost:8081/export.csv")]))
        )
        
        # Add FORMAT property
        create_table.args["properties"].append(
            "expressions", 
            FormatProperty(this=exp.Literal.string("CSV"))
        )
        
        # Generate the Greenplum SQL
        generated_sql = create_table.sql(dialect="greenplum")
        self.assertEqual(
            generated_sql, 
            "CREATE WRITABLE EXTERNAL TABLE write_table (id INT, name TEXT) LOCATION ('gpfdist://outputhost:8081/export.csv') FORMAT 'CSV'"
        ) 