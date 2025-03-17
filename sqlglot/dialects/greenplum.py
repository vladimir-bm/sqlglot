from __future__ import annotations

import typing as t
from collections import defaultdict

from sqlglot import exp, parser, tokens
from sqlglot.dialects.postgres import Postgres
from sqlglot.generator import Generator
from sqlglot.tokens import TokenType


# Define custom token types at the module level
DISTRIBUTED_BY = "DISTRIBUTED_BY"
DISTRIBUTED_RANDOMLY = "DISTRIBUTED_RANDOMLY"
ON_ALL = "ON_ALL"
ON_MASTER = "ON_MASTER"
ON_SEGMENTS = "ON_SEGMENTS"


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
    arg_types = {"this": True, "segments": False}


class FormatProperty(exp.Property):
    """FORMAT property for Greenplum external tables.
    
    Attributes:
        this: The format name (e.g., 'CSV', 'TEXT')
        options: A dictionary of format options (e.g., {'FORMATTER': 'pxfwritable_export'})
    """
    arg_types = {"this": True, "options": False}


class EncodingProperty(exp.Property):
    """ENCODING property for Greenplum external tables.
    
    Attributes:
        this: The encoding name (e.g., 'UTF8')
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
exp.Properties.NAME_TO_PROPERTY["ENCODING"] = EncodingProperty


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
            "ON ALL": ON_ALL,
            "ON MASTER": ON_MASTER,
            "ON SEGMENTS": ON_SEGMENTS,
            "ENCODING": TokenType.CHARACTER_SET,
            "FORMATTER": TokenType.PROCEDURE,
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
            "ENCODING": lambda self: self._parse_encoding(),
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
            
            segments = None
            
            # Check for segment specification (ON ALL, ON MASTER, etc.)
            if self._match(TokenType.ON):
                if self._match_texts(["ALL"]):
                    segments = "ALL"
                elif self._match_texts(["MASTER"]):
                    segments = "MASTER"
                elif self._match_texts(["SEGMENTS"]):
                    segments = "SEGMENTS"
            
            return self.expression(LocationProperty, this=exp.Array(expressions=locations), segments=segments)
        
        def _parse_format(self) -> FormatProperty:
            """Parse the FORMAT clause for external tables."""
            if not self._match(TokenType.STRING):
                self.raise_error("Expected format string after FORMAT")
            
            format_type = self.expression(exp.Literal, this=self._prev.text, is_string=True)
            
            # Check for format options in parentheses
            options = {}
            if self._match(TokenType.L_PAREN):
                # Parse format options as key-value pairs
                while True:
                    if self._match_texts("FORMATTER"):
                        if self._match(TokenType.EQ):
                            if self._match(TokenType.STRING):
                                options["FORMATTER"] = self._prev.text
                            else:
                                self.raise_error("Expected string value for FORMATTER")
                    elif self._match_texts("DELIMITER"):
                        if self._match_texts("AS") or self._match(TokenType.EQ):
                            if self._match(TokenType.STRING):
                                options["DELIMITER"] = self._prev.text
                            else:
                                self.raise_error("Expected string value for DELIMITER")
                    elif self._match_texts("NULL"):
                        if self._match_texts("AS") or self._match(TokenType.EQ):
                            if self._match(TokenType.STRING):
                                options["NULL"] = self._prev.text
                            else:
                                self.raise_error("Expected string value for NULL")
                    elif self._match_texts("HEADER"):
                        options["HEADER"] = True
                    elif self._match_texts("QUOTE"):
                        if self._match_texts("AS") or self._match(TokenType.EQ):
                            if self._match(TokenType.STRING):
                                options["QUOTE"] = self._prev.text
                            else:
                                self.raise_error("Expected string value for QUOTE")
                    elif self._match_texts("ESCAPE"):
                        if self._match_texts("AS") or self._match(TokenType.EQ):
                            if self._match(TokenType.STRING):
                                options["ESCAPE"] = self._prev.text
                            else:
                                self.raise_error("Expected string value for ESCAPE")
                    elif self._match_texts("NEWLINE"):
                        if self._match_texts("AS") or self._match(TokenType.EQ):
                            if self._match(TokenType.STRING):
                                options["NEWLINE"] = self._prev.text
                            else:
                                self.raise_error("Expected string value for NEWLINE")
                    elif self._match_texts("FILL"):
                        if self._match_texts("MISSING") and self._match_texts("FIELDS"):
                            options["FILL_MISSING_FIELDS"] = True
                    else:
                        # Try to match any other option with a value
                        if self._match(TokenType.IDENTIFIER):
                            option_name = self._prev.text.upper()
                            if self._match(TokenType.EQ):
                                if self._match(TokenType.STRING):
                                    options[option_name] = self._prev.text
                                else:
                                    self.raise_error(f"Expected string value for {option_name}")
                    
                    if self._match(TokenType.COMMA):
                        continue
                    else:
                        break
                
                if not self._match(TokenType.R_PAREN):
                    self.raise_error("Expected ')' after FORMAT options")
            
            # Only return options if we actually have some
            if not options:
                options = None
                
            return self.expression(FormatProperty, this=format_type, options=options)
        
        def _parse_encoding(self) -> EncodingProperty:
            """Parse the ENCODING clause for external tables."""
            if not self._match(TokenType.STRING):
                self.raise_error("Expected encoding string after ENCODING")
            
            encoding = self.expression(exp.Literal, this=self._prev.text, is_string=True)
            
            return self.expression(EncodingProperty, this=encoding)
        
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
            pos = self._index
            comments = self._comments.copy()
            
            # Check for READABLE/WRITABLE prefix
            if self._match_texts("READABLE"):
                self._external_readable = True
            elif self._match_texts("WRITABLE"):
                self._external_writable = True
            
            # Check for EXTERNAL
            if self._match_texts("EXTERNAL"):
                self._is_external = True
            else:
                # If we didn't find EXTERNAL, revert and continue normal parsing
                if hasattr(self, "_external_readable") or hasattr(self, "_external_writable"):
                    self._index = pos
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

        def _match_on_and_texts(self, texts):
            """Match ON followed by one of the specified texts."""
            if self._match(TokenType.ON):
                if len(texts) > 1 and self._match_texts(texts[1]):
                    return True
            return False

    class Generator(Postgres.Generator):
        # Override or extend PostgreSQL generator as needed
        
        TRANSFORMS = {
            **Postgres.Generator.TRANSFORMS,
            DistributedByProperty: lambda self, e: f"DISTRIBUTED BY ({self.expressions(e)})",
            DistributedRandomlyProperty: lambda self, e: "DISTRIBUTED RANDOMLY",
            ExternalProperty: lambda self, e: "EXTERNAL",
            WritableProperty: lambda self, e: "WRITABLE",
            ReadableProperty: lambda self, e: "READABLE",
            LocationProperty: lambda self, e: (
                f"LOCATION ({', '.join(self.sql(loc) for loc in e.this.expressions)})" + 
                (f" ON {e.segments}" if hasattr(e, 'segments') and e.segments else "")
            ),
            FormatProperty: lambda self, e: (
                f"FORMAT {self.sql(e.this)}" + 
                (f" ({self._format_options(e.options)})" if hasattr(e, 'options') and e.options else "")
            ),
            EncodingProperty: lambda self, e: f"ENCODING {self.sql(e.this)}",
        }
        
        def _format_options(self, options):
            """Format the options for FORMAT clause."""
            if not options:
                return ""
            
            parts = []
            for k, v in options.items():
                if v is True:
                    parts.append(k)
                else:
                    parts.append(f"{k}='{v}'")
            
            return ", ".join(parts)
        
        PROPERTIES_LOCATION = {
            **Postgres.Generator.PROPERTIES_LOCATION,
            DistributedByProperty: exp.Properties.Location.POST_SCHEMA,
            DistributedRandomlyProperty: exp.Properties.Location.POST_SCHEMA,
            ExternalProperty: exp.Properties.Location.POST_CREATE,
            WritableProperty: exp.Properties.Location.POST_CREATE,
            ReadableProperty: exp.Properties.Location.POST_CREATE,
            LocationProperty: exp.Properties.Location.POST_SCHEMA,
            FormatProperty: exp.Properties.Location.POST_SCHEMA,
            EncodingProperty: exp.Properties.Location.POST_SCHEMA,
        } 