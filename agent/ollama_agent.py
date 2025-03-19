import json
import requests
from sqlalchemy import text

class OllamaAgent:
    def __init__(self, engine, tables_description, model_name="llama3"):
        """
        Initialize an agent that uses Ollama for language model inference.
        
        Args:
            engine: SQLAlchemy engine for executing SQL queries
            tables_description: Description of available database tables
            model_name: Name of the Ollama model to use (default: llama3)
        """
        self.engine = engine
        self.tables_description = tables_description
        self.model_name = model_name
        self.ollama_url = "http://localhost:11434/api/generate"
    
    def sql_engine(self, query):
        """
        Execute a SQL query and return the results
        """
        output = ""
        try:
            with self.engine.connect() as con:
                rows = con.execute(text(query))
                for row in rows:
                    output += "\n" + str(row)
            return output.strip() if output else "Query executed successfully. No rows returned."
        except Exception as e:
            return f"Error executing query: {str(e)}"
    
    def generate_prompt(self, user_query):
        """
        Generate a prompt for the language model
        """
        return f"""You are a helpful SQL assistant. 
        
Database information:
{self.tables_description}

User's question: {user_query}

First, think through what SQL query would answer this question. 
Then, provide the exact SQL query. Format your final SQL query between triple backticks like:
```sql
SELECT * FROM example_table;
```

Think carefully about the query. Do not include any explanations or additional content in your response, ONLY output the SQL query between triple backticks."""
    
    def extract_sql_query(self, response):
        """Extract SQL query from model response"""
        try:
            # Look for SQL query between ```sql and ``` markers
            if "```sql" in response and "```" in response.split("```sql")[1]:
                sql_part = response.split("```sql")[1].split("```")[0].strip()
                return sql_part
            
            # Alternative pattern: just between ``` markers
            elif "```" in response and "```" in response.split("```")[1]:
                sql_part = response.split("```")[1].split("```")[0].strip()
                return sql_part
                
            # Fallback: just return the whole response
            return response.strip()
        except Exception:
            return response.strip()
    
    def run(self, query):
        """
        Run the agent with a natural language query
        
        Args:
            query: Natural language query from the user
            
        Returns:
            Dictionary with the original query, generated SQL, and results
        """
        try:
            # 1. Generate prompt for Ollama
            prompt = self.generate_prompt(query)
            
            # 2. Call Ollama API
            response = requests.post(
                self.ollama_url,
                json={
                    "model": self.model_name,
                    "prompt": prompt,
                    "stream": False
                }
            )
            
            if response.status_code != 200:
                return f"Error calling Ollama API: {response.text}"
            
            # 3. Extract the generated text
            generated_text = response.json().get("response", "")
            
            # 4. Extract SQL query from the response
            sql_query = self.extract_sql_query(generated_text)
            
            # 5. Execute the SQL query
            result = self.sql_engine(sql_query)
            
            # 6. Format and return the response
            return {
                "user_query": query,
                "sql_query": sql_query,
                "result": result
            }
            
        except Exception as e:
            return f"Error running Ollama agent: {str(e)}"