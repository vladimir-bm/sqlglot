import sqlglot
from sqlglot import exp, parse_one
from sqlglot.dialects.greenplum import (
    DistributedByProperty, 
    DistributedRandomlyProperty,
    ExternalProperty,
    WritableProperty,
    ReadableProperty,
    LocationProperty,
    FormatProperty,
    EncodingProperty,
    Greenplum
)
from tests.dialects.test_dialect import Validator


class TestGreenplum(Validator):
    """
    Test for Greenplum dialect functionality.
    """
    maxDiff = None
    dialect = "greenplum"
    
    def setUp(self):
        """Setup for Greenplum tests."""
        # Ensure the property classes are registered in PROPERTIES_LOCATION
        if hasattr(Greenplum, 'Generator'):
            if not hasattr(Greenplum.Generator, 'PROPERTIES_LOCATION'):
                Greenplum.Generator.PROPERTIES_LOCATION = {}
            
            Greenplum.Generator.PROPERTIES_LOCATION.update({
                LocationProperty: exp.Properties.Location.POST_SCHEMA,
                FormatProperty: exp.Properties.Location.POST_SCHEMA,
                EncodingProperty: exp.Properties.Location.POST_SCHEMA,
            })

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
        # Minimal test case
        formats_sql = "CREATE EXTERNAL TABLE ext_table (id INT) FORMAT 'CSV'"
        self.assertEqual(
            sqlglot.transpile(formats_sql, read="greenplum", write="greenplum")[0],
            formats_sql
        )
        
        # Test with LOCATION
        location_sql = "CREATE EXTERNAL TABLE ext_table (id INT) LOCATION ('file://host/path/file.csv') FORMAT 'CSV'"
        self.assertEqual(
            sqlglot.transpile(location_sql, read="greenplum", write="greenplum")[0],
            location_sql
        )
        
        # Test with multiple locations
        multi_loc_sql = "CREATE EXTERNAL TABLE ext_table (id INT) LOCATION ('file://host1/path/file1.csv', 'file://host2/path/file2.csv') FORMAT 'CSV'"
        self.assertEqual(
            sqlglot.transpile(multi_loc_sql, read="greenplum", write="greenplum")[0],
            multi_loc_sql
        )
        
        # Test with READABLE keyword
        readable_sql = "CREATE READABLE EXTERNAL TABLE ext_table (id INT) LOCATION ('file://host/path/file.csv') FORMAT 'CSV'"
        self.assertEqual(
            sqlglot.transpile(readable_sql, read="greenplum", write="greenplum")[0],
            readable_sql
        )
        
        # Test with ON ALL clause
        on_all_sql = "CREATE EXTERNAL TABLE ext_table (id INT) LOCATION ('file://host/path/file.csv') ON ALL FORMAT 'CSV'"
        self.assertEqual(
            sqlglot.transpile(on_all_sql, read="greenplum", write="greenplum")[0],
            on_all_sql
        )
        
    def test_writable_external_table(self):
        """Test Greenplum's WRITABLE EXTERNAL TABLE clause."""
        # Test writable external table
        sql_writable = "CREATE WRITABLE EXTERNAL TABLE write_table (id INT, name TEXT) LOCATION ('gpfdist://outputhost:8081/export.csv') FORMAT 'CSV'"
        
        self.assertEqual(
            sqlglot.transpile(sql_writable, read="greenplum", write="greenplum")[0],
            sql_writable
        )
        
        # Test with complex example including all features
        sql_complex = """CREATE WRITABLE EXTERNAL TABLE write_table 
            (id INT, name TEXT) 
            LOCATION ('pxf://dm_udh_dashboard_grr.kp_weather_d?profile=JDBC&SERVER=cl_dashboard_grr&BATCH_SIZE=100000') 
            ON ALL 
            FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export') 
            ENCODING 'UTF8'"""
        
        # Remove whitespace to normalize the SQL for comparison
        normalized_sql = ' '.join(sql_complex.split())
        
        self.assertEqual(
            ' '.join(sqlglot.transpile(normalized_sql, read="greenplum", write="greenplum")[0].split()),
            normalized_sql
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
            LocationProperty(this=exp.Array(expressions=[exp.Literal.string("gpfdist://outputhost:8081/export.csv")]), segments=None)
        )
        
        # Add FORMAT property
        create_table.args["properties"].append(
            "expressions", 
            FormatProperty(this=exp.Literal.string("CSV"), options=None)
        )
        
        # Generate the Greenplum SQL
        generated_sql = create_table.sql(dialect="greenplum")
        self.assertEqual(
            generated_sql, 
            "CREATE WRITABLE EXTERNAL TABLE write_table (id INT, name TEXT) LOCATION ('gpfdist://outputhost:8081/export.csv') FORMAT 'CSV'"
        )
        
    def test_full_external_table_example(self):
        """Test full external table example with all features."""
        # This is the example provided by the user
        sql = """
        create writable external table schema.table (
        col1 text,
        col2 numeric,
        col3 date,
        col4 timestamp
        )
        LOCATION (
        'pxf://connector?profile=JDBC&SERVER=server&BATCH_SIZE=100000'
        ) ON ALL
        FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_export' )
        ENCODING 'UTF8';
        """
        
        # Normalize the SQL for comparison (remove whitespace)
        normalized_sql = ' '.join(sql.strip().split())
        
        # Parse and generate
        parsed = parse_one(sql, dialect="greenplum")
        generated = parsed.sql(dialect="greenplum")
        
        # Normalize the generated SQL
        normalized_generated = ' '.join(generated.strip().split())
        
        # Compare normalized versions
        self.assertEqual(normalized_generated, normalized_sql.rstrip(";")) 