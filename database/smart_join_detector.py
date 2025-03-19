from sqlalchemy import create_engine, inspect, text, Table, MetaData
import re

class SmartJoinDetector:
    """
    A class that intelligently detects potential relationships between database tables
    without relying solely on foreign key constraints.
    """
    
    def __init__(self, engine):
        self.engine = engine
        self.inspector = inspect(engine)
        self.metadata = MetaData()
        self.metadata.reflect(bind=engine)
        self.tables_info = self._get_tables_info()
        self.foreign_keys = self._get_foreign_keys()
        self.potential_relationships = self._discover_potential_relationships()
    
    def _get_tables_info(self):
        """Get detailed information about all tables in the database"""
        tables_info = {}
        
        for table_name in self.inspector.get_table_names():
            # Get columns
            columns = []
            for column in self.inspector.get_columns(table_name):
                columns.append({
                    "name": column["name"],
                    "type": str(column["type"]),
                    "nullable": column.get("nullable", True)
                })
            
            # Get primary key
            pk_constraint = self.inspector.get_pk_constraint(table_name)
            primary_keys = pk_constraint.get("constrained_columns", [])
            
            # Store table info
            tables_info[table_name] = {
                "columns": columns,
                "primary_keys": primary_keys,
                "column_names": [col["name"] for col in columns]
            }
        
        return tables_info
    
    def _get_foreign_keys(self):
        """Get declared foreign key relationships from the database"""
        foreign_keys = {}
        
        for table_name in self.inspector.get_table_names():
            fks = self.inspector.get_foreign_keys(table_name)
            
            if fks:
                foreign_keys[table_name] = []
                
                for fk in fks:
                    foreign_keys[table_name].append({
                        'constrained_columns': fk['constrained_columns'],
                        'referred_table': fk['referred_table'],
                        'referred_columns': fk['referred_columns']
                    })
        
        return foreign_keys
    
    def _discover_potential_relationships(self):
        """
        Discover potential relationships between tables using naming conventions and patterns,
        even when foreign keys are not explicitly defined.
        """
        potential_relationships = {}
        
        for source_table, source_info in self.tables_info.items():
            potential_relationships[source_table] = []
            
            # Check each table against the source table
            for target_table, target_info in self.tables_info.items():
                if source_table == target_table:
                    continue
                
                # Look for relationships between these tables
                relationships = self._find_relationships(source_table, target_table, 
                                                        source_info, target_info)
                
                if relationships:
                    potential_relationships[source_table].extend(relationships)
        
        return potential_relationships
    
    def _find_relationships(self, source_table, target_table, source_info, target_info):
        """Find potential relationships between two tables based on column naming patterns"""
        relationships = []
        source_cols = source_info["column_names"]
        target_cols = target_info["column_names"]
        target_pk = target_info["primary_keys"][0] if target_info["primary_keys"] else "id"
        source_pk = source_info["primary_keys"][0] if source_info["primary_keys"] else "id"
        
        # Pattern 1: column named exactly like target_table_id
        expected_fk = f"{target_table}_id"
        if expected_fk in source_cols:
            relationships.append({
                "source_table": source_table,
                "source_column": expected_fk,
                "target_table": target_table,
                "target_column": target_pk,
                "confidence": "high"
            })
            return relationships  # Return early as this is the strongest evidence
        
        # Pattern 2: column named like singular form of target_table_id
        singular_fk = f"{self._singularize(target_table)}_id"
        if singular_fk in source_cols:
            relationships.append({
                "source_table": source_table,
                "source_column": singular_fk,
                "target_table": target_table,
                "target_column": target_pk,
                "confidence": "high"
            })
            return relationships  # Return early as this is also strong evidence
        
        # Pattern 3: column named exactly like target_table (without _id)
        if target_table in source_cols:
            relationships.append({
                "source_table": source_table,
                "source_column": target_table,
                "target_table": target_table,
                "target_column": target_pk,
                "confidence": "medium"
            })
            return relationships
        
        # Pattern 4: singular form of target table name as column
        singular_name = self._singularize(target_table)
        if singular_name in source_cols:
            relationships.append({
                "source_table": source_table,
                "source_column": singular_name,
                "target_table": target_table,
                "target_column": target_pk,
                "confidence": "medium"
            })
            return relationships
        
        # Pattern 5: target table's primary key name (if not 'id') appears as column
        if target_pk != "id" and target_pk in source_cols:
            relationships.append({
                "source_table": source_table,
                "source_column": target_pk,
                "target_table": target_table,
                "target_column": target_pk,
                "confidence": "medium"
            })
            return relationships
        
        # Pattern 6: Common naming patterns like 'parent_id', 'foreign_id', etc.
        common_patterns = [
            "parent_id", "child_id", "foreign_id", "related_id", 
            "parent", "owner_id", "owner", "user_id", "user"
        ]
        
        # Check if source has any of these common patterns and target has primary key 
        for pattern in common_patterns:
            if pattern in source_cols and target_pk in target_cols:
                relationships.append({
                    "source_table": source_table,
                    "source_column": pattern,
                    "target_table": target_table,
                    "target_column": target_pk,
                    "confidence": "low"
                })
        
        # Pattern 7: Matching column names between tables (same name)
        for source_col in source_cols:
            if source_col in target_cols and source_col not in ["id", "created_at", "updated_at"]:
                relationships.append({
                    "source_table": source_table,
                    "source_column": source_col,
                    "target_table": target_table,
                    "target_column": source_col,
                    "confidence": "low"
                })
        
        return relationships
    
    def _singularize(self, word):
        """Convert a potentially plural English word to singular form using common rules"""
        if word.endswith('ies'):
            return word[:-3] + 'y'
        elif word.endswith('es'):
            return word[:-2]
        elif word.endswith('s') and not word.endswith('ss'):
            return word[:-1]
        return word
    
    def get_relationships_for_table(self, table_name):
        """Get all potential relationships for a specific table"""
        if table_name not in self.tables_info:
            return []
            
        # First, check explicit foreign keys
        explicit_relationships = []
        if table_name in self.foreign_keys:
            for fk in self.foreign_keys[table_name]:
                explicit_relationships.append({
                    "source_table": table_name,
                    "source_column": fk['constrained_columns'][0],  # Simplify to first column
                    "target_table": fk['referred_table'],
                    "target_column": fk['referred_columns'][0],  # Simplify to first column
                    "confidence": "explicit"
                })
        
        # Then, add discovered potential relationships
        potential_rels = self.potential_relationships.get(table_name, [])
        
        # Combine and deduplicate
        all_relationships = explicit_relationships.copy()
        
        # Only add potential relationships that don't already exist as explicit FKs
        for pot_rel in potential_rels:
            is_duplicate = False
            for exp_rel in explicit_relationships:
                if (pot_rel["source_column"] == exp_rel["source_column"] and 
                    pot_rel["target_table"] == exp_rel["target_table"]):
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                all_relationships.append(pot_rel)
        
        return all_relationships
    
    def generate_join_query(self, main_table, include_columns=True):
        """Generate a JOIN query for the main table with all related tables"""
        if main_table not in self.tables_info:
            return f"-- Table '{main_table}' not found in database"
        
        # Get all relationships for this table
        relationships = self.get_relationships_for_table(main_table)
        
        # If no relationships found, return a simple query
        if not relationships:
            return f"SELECT * FROM {main_table};"
        
        # Create aliases for tables (using first letter or first+number for duplicates)
        table_aliases = {main_table: main_table[0]}
        alias_count = {}
        
        # Sort relationships by confidence
        confidence_rank = {"explicit": 4, "high": 3, "medium": 2, "low": 1}
        relationships.sort(key=lambda r: confidence_rank.get(r["confidence"], 0), reverse=True)
        
        # Create JOIN clauses
        joins = []
        selected_columns = [f"{table_aliases[main_table]}.*"]
        
        for rel in relationships:
            target_table = rel["target_table"]
            
            # Generate alias
            if target_table not in table_aliases:
                if target_table[0] not in alias_count:
                    alias_count[target_table[0]] = 0
                    table_aliases[target_table] = target_table[0]
                else:
                    alias_count[target_table[0]] += 1
                    table_aliases[target_table] = f"{target_table[0]}{alias_count[target_table[0]]}"
            
            target_alias = table_aliases[target_table]
            main_alias = table_aliases[main_table]
            
            # Create JOIN clause
            join_clause = f"LEFT JOIN {target_table} AS {target_alias} ON {main_alias}.{rel['source_column']} = {target_alias}.{rel['target_column']}"
            joins.append(join_clause)
            
            # Add selected columns from target table if requested
            if include_columns:
                for col in self.tables_info[target_table]["column_names"]:
                    # Skip the join column to avoid duplication
                    if col != rel["target_column"]:
                        selected_columns.append(f"{target_alias}.{col} AS {target_table}_{col}")
        
        # Construct the final query
        if include_columns:
            query = f"SELECT {', '.join(selected_columns)}\nFROM {main_table} AS {table_aliases[main_table]}"
        else:
            query = f"SELECT {table_aliases[main_table]}.*\nFROM {main_table} AS {table_aliases[main_table]}"
        
        for join in joins:
            query += f"\n{join}"
        
        return query + ";"
    
    def validate_join_conditions(self, query):
        """
        Validate JOIN conditions in a query against actual database schema
        Returns corrected query if issues found, or original query if valid
        """
        # Extract join conditions from the query
        join_pattern = r'JOIN\s+(\w+)(?:\s+AS\s+(\w+))?\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
        matches = re.findall(join_pattern, query, re.IGNORECASE)
        
        if not matches:
            return query  # No joins found
        
        # Check each join condition
        valid_query = query
        for match in matches:
            table_name, alias1, alias2, col1, alias3, col2 = match
            
            # Determine which aliases correspond to which tables
            table1 = None
            table2 = None
            
            # First, check if an explicit alias is defined in the match
            if alias1:
                # alias1 is the alias for the joined table
                alias_of_joined_table = alias1
                joined_table_name = table_name
            else:
                # If no alias was defined, the matched table name is both the table and its own alias
                alias_of_joined_table = table_name
                joined_table_name = table_name
            
            # Now determine which table each column belongs to by checking if the column exists
            if joined_table_name in self.tables_info:
                if col1 in self.tables_info[joined_table_name]["column_names"]:
                    # The first column belongs to the joined table, so alias2 must be the joined table's alias
                    # and alias3 must be the other table's alias
                    if alias2 == alias_of_joined_table:
                        table1 = joined_table_name
                        # Try to determine table2 from alias3
                        for t_name in self.tables_info:
                            # Quick heuristic: if alias is first letter of table name
                            if t_name.startswith(alias3):
                                table2 = t_name
                                break
                    
                if col2 in self.tables_info[joined_table_name]["column_names"]:
                    # The second column belongs to the joined table, so alias3 must be the joined table's alias
                    # and alias2 must be the other table's alias
                    if alias3 == alias_of_joined_table:
                        table2 = joined_table_name
                        # Try to determine table1 from alias2
                        for t_name in self.tables_info:
                            # Quick heuristic: if alias is first letter of table name
                            if t_name.startswith(alias2):
                                table1 = t_name
                                break
            
            # If we couldn't determine the tables, try a more general approach
            if not table1 or not table2:
                for t_name in self.tables_info:
                    # Check all tables to see which one the columns might belong to
                    if col1 in self.tables_info[t_name]["column_names"] and alias2.startswith(t_name[0]):
                        table1 = t_name
                    if col2 in self.tables_info[t_name]["column_names"] and alias3.startswith(t_name[0]):
                        table2 = t_name
            
            # Check if columns exist in their respective tables
            col1_exists = False
            col2_exists = False
            
            if table1 and col1 in self.tables_info.get(table1, {}).get("column_names", []):
                col1_exists = True
            
            if table2 and col2 in self.tables_info.get(table2, {}).get("column_names", []):
                col2_exists = True
            
            # If both columns exist, this join condition is valid
            if col1_exists and col2_exists:
                continue
            
            # Otherwise, try to fix the invalid join condition
            if table1 and table2:
                fixed_join = self._fix_join_condition(table1, alias2, table2, alias3, col1, col2)
                if fixed_join:
                    # Replace the invalid join with the fixed one
                    original_join = f"JOIN {table_name}"
                    if alias1:
                        original_join += f" AS {alias1}"
                    original_join += f" ON {alias2}.{col1} = {alias3}.{col2}"
                    
                    # Replace in the query (only the first occurrence to avoid incorrect replacements)
                    valid_query = valid_query.replace(original_join, fixed_join, 1)
        
        return valid_query
    
    def _fix_join_condition(self, table1, alias1, table2, alias2, col1, col2):
        """Try to fix an invalid join condition by finding alternative columns"""
        # Find relationships between these tables
        relationships_t1_to_t2 = []
        relationships_t2_to_t1 = []
        
        # Get relationships in both directions
        for rel in self.get_relationships_for_table(table1):
            if rel["target_table"] == table2:
                relationships_t1_to_t2.append(rel)
        
        for rel in self.get_relationships_for_table(table2):
            if rel["target_table"] == table1:
                relationships_t2_to_t1.append(rel)
        
        # Sort by confidence
        confidence_rank = {"explicit": 4, "high": 3, "medium": 2, "low": 1}
        
        if relationships_t1_to_t2:
            relationships_t1_to_t2.sort(key=lambda r: confidence_rank.get(r["confidence"], 0), reverse=True)
        
        if relationships_t2_to_t1:
            relationships_t2_to_t1.sort(key=lambda r: confidence_rank.get(r["confidence"], 0), reverse=True)
        
        # Try to find a replacement join condition
        if relationships_t1_to_t2:
            # Use the highest confidence relationship from table1 to table2
            rel = relationships_t1_to_t2[0]
            return f"JOIN {table2} AS {alias2} ON {alias1}.{rel['source_column']} = {alias2}.{rel['target_column']}"
        
        elif relationships_t2_to_t1:
            # Use the highest confidence relationship from table2 to table1
            rel = relationships_t2_to_t1[0]
            return f"JOIN {table2} AS {alias2} ON {alias2}.{rel['source_column']} = {alias1}.{rel['target_column']}"
        
        # If no relationship found, try common patterns
        
        # 1. Try standard naming convention: table2_id in table1
        expected_fk = f"{table2}_id"
        if expected_fk in self.tables_info.get(table1, {}).get("column_names", []):
            return f"JOIN {table2} AS {alias2} ON {alias1}.{expected_fk} = {alias2}.id"
        
        # 2. Try singular form
        singular_name = self._singularize(table2)
        singular_fk = f"{singular_name}_id"
        if singular_fk in self.tables_info.get(table1, {}).get("column_names", []):
            return f"JOIN {table2} AS {alias2} ON {alias1}.{singular_fk} = {alias2}.id"
        
        # 3. Try direct name reference (e.g., 'role' column referencing 'roles' table)
        if singular_name in self.tables_info.get(table1, {}).get("column_names", []):
            return f"JOIN {table2} AS {alias2} ON {alias1}.{singular_name} = {alias2}.id"
        
        # 4. Check if table1 has an 'id' column that table2 might reference
        if "id" in self.tables_info.get(table1, {}).get("column_names", []):
            table1_fk = f"{table1}_id"
            if table1_fk in self.tables_info.get(table2, {}).get("column_names", []):
                return f"JOIN {table2} AS {alias2} ON {alias1}.id = {alias2}.{table1_fk}"
            
            # Try singular form
            singular_table1 = self._singularize(table1)
            singular_table1_fk = f"{singular_table1}_id"
            if singular_table1_fk in self.tables_info.get(table2, {}).get("column_names", []):
                return f"JOIN {table2} AS {alias2} ON {alias1}.id = {alias2}.{singular_table1_fk}"
        
        # No valid fix found
        return None