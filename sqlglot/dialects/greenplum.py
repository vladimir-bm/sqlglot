from __future__ import annotations

import typing as t

from sqlglot import exp, parser, tokens
from sqlglot.dialects.postgres import Postgres
from sqlglot.generator import Generator
from sqlglot.tokens import TokenType


# Define custom token types at the module level
DISTRIBUTED_BY = "DISTRIBUTED_BY"
DISTRIBUTED_RANDOMLY = "DISTRIBUTED_RANDOMLY"


# Custom property classes for Greenplum
class DistributedByProperty(exp.Property):
    arg_types = {"expressions": True}


class DistributedRandomlyProperty(exp.Property):
    arg_types = {"this": False}


class ExternalProperty(exp.Property):
    arg_types = {"this": False}


class WritableProperty(exp.Property):
    arg_types = {"this": False}


class ReadableProperty(exp.Property):
    arg_types = {"this": False}


class LocationProperty(exp.Property):
    arg_types = {"this": True}


class FormatProperty(exp.Property):
    """FORMAT property for Greenplum external tables.
    
    Attributes:
        this: The format name (e.g., 'CSV', 'TEXT')
    """
    arg_types = {"this": True}


# Register the property classes in the Properties.NAME_TO_PROPERTY mapping
exp.Properties.NAME_TO_PROPERTY["DISTRIBUTED BY"] = DistributedByProperty
exp.Properties.NAME_TO_PROPERTY["DISTRIBUTED RANDOMLY"] = DistributedRandomlyProperty
exp.Properties.NAME_TO_PROPERTY["EXTERNAL"] = ExternalProperty
exp.Properties.NAME_TO_PROPERTY["WRITABLE"] = WritableProperty
exp.Properties.NAME_TO_PROPERTY["READABLE"] = ReadableProperty
exp.Properties.NAME_TO_PROPERTY["LOCATION"] = LocationProperty
exp.Properties.NAME_TO_PROPERTY["FORMAT"] = FormatProperty


class Greenplum(Postgres):
    """
    Greenplum dialect, based on Postgres.
    
    Greenplum is a massively parallel processing (MPP) database server that is based on PostgreSQL.
    It extends PostgreSQL with specialized features for data distribution and parallel query execution.
    """
    
    class Tokenizer(Postgres.Tokenizer):
        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,
            "DISTRIBUTED BY": DISTRIBUTED_BY,
            "DISTRIBUTED RANDOMLY": DISTRIBUTED_RANDOMLY,
            # Reuse existing tokens for keywords that aren't in TokenType
            "EXTERNAL": TokenType.CREATE,
            "LOCATION": TokenType.FROM,
            "FORMAT": TokenType.FORMAT,
            "WRITABLE": TokenType.CREATE, 
            "READABLE": TokenType.CREATE,
        }
    
    class Parser(Postgres.Parser):
        # Define property parsers for Greenplum-specific properties
        PROPERTY_PARSERS = {
            **Postgres.Parser.PROPERTY_PARSERS,
            "DISTRIBUTED BY": lambda self: self._parse_distributed_by(),
            "DISTRIBUTED RANDOMLY": lambda self: self.expression(
                DistributedRandomlyProperty
            ),
            "LOCATION": lambda self: self._parse_location(),
            "FORMAT": lambda self: self._parse_format(),
        }
        
        def _parse_distributed_by(self) -> DistributedByProperty:
            """Parse the DISTRIBUTED BY clause."""
            if not self._match(TokenType.L_PAREN):
                self.raise_error("Expected '(' after DISTRIBUTED BY")
                
            exprs = self._parse_csv(self._parse_id_var)
            
            if not self._match(TokenType.R_PAREN):
                self.raise_error("Expected ')' after DISTRIBUTED BY column list")
                
            return self.expression(DistributedByProperty, expressions=exprs)
        
        def _parse_location(self) -> LocationProperty:
            """Parse the LOCATION clause for external tables."""
            if not self._match(TokenType.L_PAREN):
                self.raise_error("Expected '(' after LOCATION")
            
            locations = []
            while True:
                if self._match(TokenType.STRING):
                    locations.append(self.expression(exp.Literal, this=self._prev.text, is_string=True))
                    
                if self._match(TokenType.COMMA):
                    continue
                else:
                    break
            
            if not self._match(TokenType.R_PAREN):
                self.raise_error("Expected ')' after LOCATION parameters")
            
            if not locations:
                self.raise_error("Expected at least one location in LOCATION clause")
            
            return self.expression(LocationProperty, this=exp.Array(expressions=locations))

        def _parse_format(self) -> FormatProperty:
            """Parse the FORMAT clause for external tables."""
            if not self._match(TokenType.STRING):
                self.raise_error("Expected format string after FORMAT")
            
            format_type = self.expression(exp.Literal, this=self._prev.text, is_string=True)
            
            # We'll simplify by not parsing format options specifically
            # We'll just return the format type
            return self.expression(FormatProperty, this=format_type)
        
        def _parse_create(self):
            """Override _parse_create to handle EXTERNAL TABLE creation"""
            create = super()._parse_create()
            
            # Handle external tables if this is a table create
            if hasattr(create, "kind") and create.kind == "TABLE" and not isinstance(create.this, exp.Anonymous):
                # Check if we have recorded the external table properties
                if hasattr(self, "_is_external") and self._is_external:
                    if not create.args.get("properties"):
                        create.set("properties", exp.Properties(expressions=[]))
                    
                    create.args["properties"].append(
                        "expressions", ExternalProperty()
                    )
                    
                    # Check for READABLE/WRITABLE that would have been processed before
                    if hasattr(self, "_external_readable") and self._external_readable:
                        create.args["properties"].append(
                            "expressions", ReadableProperty()
                        )
                        delattr(self, "_external_readable")
                    
                    if hasattr(self, "_external_writable") and self._external_writable:
                        create.args["properties"].append(
                            "expressions", WritableProperty()
                        )
                        delattr(self, "_external_writable")
                    
                    delattr(self, "_is_external")
            
            return create
        
        def _parse_table_create(self):
            """Override _parse_table_create to handle EXTERNAL TABLE prefixes"""
            # Capture original position to allow backtracking
            pos = self._i
            comments = self._comments.copy()
            
            # Check for READABLE/WRITABLE prefix
            if self._match_text("READABLE"):
                self._external_readable = True
            elif self._match_text("WRITABLE"):
                self._external_writable = True
            
            # Check for EXTERNAL
            if self._match_text("EXTERNAL"):
                self._is_external = True
            else:
                # If we didn't find EXTERNAL, revert and continue normal parsing
                if hasattr(self, "_external_readable") or hasattr(self, "_external_writable"):
                    self._i = pos
                    self._comments = comments
            
            # Now continue with normal table creation
            return super()._parse_table_create()
        
        def _parse_statement(self):
            try:
                # Try normal statement parsing
                return super()._parse_statement()
            except Exception as e:
                # If this is an external table with FORMAT, try special handling
                if (hasattr(self, "_is_external") and self._is_external):
                    # Handle format clause here
                    # We won't implement this fallback for now
                    # as it requires more complex handling
                    pass
                # Re-raise the exception
                raise e

    class Generator(Postgres.Generator):
        # Override or extend PostgreSQL generator as needed
        
        TRANSFORMS = {
            **Postgres.Generator.TRANSFORMS,
            DistributedByProperty: lambda self, e: f"DISTRIBUTED BY ({self.expressions(e)})",
            DistributedRandomlyProperty: lambda self, e: "DISTRIBUTED RANDOMLY",
            ExternalProperty: lambda self, e: "EXTERNAL",
            WritableProperty: lambda self, e: "WRITABLE",
            ReadableProperty: lambda self, e: "READABLE",
            LocationProperty: lambda self, e: f"LOCATION ({', '.join(self.sql(loc) for loc in e.this.expressions)})",
            FormatProperty: lambda self, e: f"FORMAT {self.sql(e.this)}",
        }
        
        PROPERTIES_LOCATION = {
            **Postgres.Generator.PROPERTIES_LOCATION,
            DistributedByProperty: exp.Properties.Location.POST_SCHEMA,
            DistributedRandomlyProperty: exp.Properties.Location.POST_SCHEMA,
            ExternalProperty: exp.Properties.Location.POST_CREATE,
            WritableProperty: exp.Properties.Location.POST_CREATE,
            ReadableProperty: exp.Properties.Location.POST_CREATE,
            LocationProperty: exp.Properties.Location.POST_SCHEMA,
            FormatProperty: exp.Properties.Location.POST_SCHEMA,
        } 