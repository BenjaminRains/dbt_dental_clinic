# Python Standard Library Reference Guide

A comprehensive reference of Python standard library methods and patterns used throughout the dbt_dental_clinic codebase.

## Overview

This guide covers **28 topics** with real examples from your codebase, including:

✅ **Core modules**: `os`, `pathlib`, `logging`, `datetime`, `time`, `sys`  
✅ **Data structures**: `collections`, `enum`, `decimal`, `dataclasses`  
✅ **Data formats**: `json`, `yaml`  
✅ **Advanced patterns**: Special methods, inheritance, decorators, list operations  
✅ **Common libraries**: Pydantic for data validation  

**NEW in this expanded version:**
- 🆕 `random` - Random number generation for testing and synthetic data
- 🆕 `json` & `yaml` - Working with data formats
- 🆕 `decimal` - Precise financial calculations
- 🆕 `collections` - defaultdict, Counter, namedtuple
- 🆕 `functools` - Decorators, caching, and functional programming
- 🆕 `dataclasses` - Simplified class creation
- 🆕 Special methods (`__init__`, `__str__`, etc.)
- 🆕 Class inheritance and `super()`
- 🆕 Advanced list operations (slicing, unpacking)
- 🆕 Pydantic for API data validation
- 🆕 `sys` module for system operations

---

## Table of Contents

**Core Standard Library:**
1. [os - Operating System Interface](#os---operating-system-interface)
2. [pathlib - Object-oriented File Paths](#pathlib---object-oriented-file-paths)
3. [logging - Flexible Event Logging](#logging---flexible-event-logging)
4. [datetime - Date and Time Types](#datetime---date-and-time-types)
5. [time - Time Access and Conversions](#time---time-access-and-conversions)
6. [sys - System-specific Parameters](#sys---system-specific-parameters)

**Data Types & Structures:**
7. [enum - Enumeration Support](#enum---enumeration-support)
8. [typing - Type Hints](#typing---type-hints)
9. [collections - Specialized Container Types](#collections---specialized-container-types)
10. [decimal - Decimal Fixed Point Arithmetic](#decimal---decimal-fixed-point-arithmetic)
11. [dataclasses - Data Classes](#dataclasses---data-classes)

**Data Formats:**
12. [json - JSON Encoding and Decoding](#json---json-encoding-and-decoding)
13. [yaml - YAML Parser](#yaml---yaml-parser)

**Randomness & Testing:**
14. [random - Random Number Generation](#random---random-number-generation)

**Functions & Advanced Patterns:**
15. [functools - Higher-order Functions](#functools---higher-order-functions)
16. [Special Methods (Dunder Methods)](#special-methods-dunder-methods)
17. [Class Inheritance and super()](#class-inheritance-and-super)

**String Operations:**
18. [String Methods](#string-methods)
19. [String Formatting](#string-formatting)

**Type Conversions & Operations:**
20. [Type Conversions](#type-conversions)
21. [Built-in Functions](#built-in-functions)
22. [List Comprehensions](#list-comprehensions)
23. [Advanced List Operations](#advanced-list-operations)
24. [Dictionary Operations](#dictionary-operations)

**Error Handling & Resources:**
25. [Error Handling](#error-handling)
26. [Context Managers](#context-managers)
27. [Global Variables](#global-variables)

**External Libraries (Commonly Used):**
28. [Pydantic - Data Validation](#pydantic---data-validation)

---

## os - Operating System Interface

**Purpose:** Interact with the operating system (environment variables, file system, processes)

**Official Docs:** https://docs.python.org/3/library/os.html

### From Codebase: `api/config.py`

```python
import os

# Get environment variable (returns None if not found)
environment = os.getenv('API_ENVIRONMENT')

# Get environment variable with default value
debug_mode = os.getenv('DEBUG', 'false')

# Get environment variable (raises KeyError if not found)
required_var = os.environ['REQUIRED_VAR']
```

### Common Methods:
- `os.getenv(key, default=None)` - Get environment variable
- `os.environ[key]` - Access environment variables as dict
- `os.path.exists(path)` - Check if path exists
- `os.path.join(path1, path2)` - Join path components
- `os.listdir(path)` - List directory contents
- `os.getcwd()` - Get current working directory

### When to Use:
- Reading configuration from environment variables
- Checking if files/directories exist (though `pathlib` is preferred)
- Getting current directory
- System-level operations

---

## pathlib - Object-oriented File Paths

**Purpose:** Modern, object-oriented approach to file system paths

**Official Docs:** https://docs.python.org/3/library/pathlib.html

### From Codebase: `api/config.py`

```python
from pathlib import Path

# Get directory of current file
current_dir = Path(__file__).parent

# Navigate to parent directory
project_root = current_dir.parent

# Build paths with / operator
env_file = project_root / f".env_api_{self.environment}"

# Check if file exists
if env_file.exists():
    # Do something

# Get absolute path
absolute_path = env_file.resolve()
```

### Common Methods:
- `Path(__file__)` - Path to current Python file
- `.parent` - Parent directory
- `.exists()` - Check if path exists
- `.is_file()` - Check if path is a file
- `.is_dir()` - Check if path is a directory
- `.resolve()` - Get absolute path
- `/` operator - Join paths (e.g., `path1 / path2`)

### When to Use:
- File path operations (preferred over `os.path`)
- Checking if files exist
- Navigating directory structures
- Reading/writing files

### Why Better Than os.path:
```python
# Old way (os.path)
import os
path = os.path.join(os.path.dirname(__file__), '..', 'data', 'file.txt')

# Modern way (pathlib)
from pathlib import Path
path = Path(__file__).parent.parent / 'data' / 'file.txt'
```

---

## logging - Flexible Event Logging

**Purpose:** Track events, errors, and debug information

**Official Docs:** https://docs.python.org/3/library/logging.html

### From Codebase: `api/database.py`, `etl_pipeline/`

```python
import logging

# Create logger for current module
logger = logging.getLogger(__name__)

# Log different severity levels
logger.debug("Detailed information for debugging")
logger.info("General informational message")
logger.warning("Warning - something unexpected")
logger.error("Error occurred but program continues")
logger.critical("Critical error - program may not continue")

# Log with variables
logger.info(f"Connecting to database for environment: {config.environment}")

# Log exceptions
try:
    risky_operation()
except Exception as e:
    logger.exception("Operation failed", exc_info=True)
```

### Logging Levels (lowest to highest):
1. `DEBUG` (10) - Detailed diagnostic information
2. `INFO` (20) - Confirmation that things are working
3. `WARNING` (30) - Something unexpected happened
4. `ERROR` (40) - A serious problem occurred
5. `CRITICAL` (50) - Program may not be able to continue

### Common Patterns:
```python
# Get logger for current module
logger = logging.getLogger(__name__)

# Configure logging (usually in main script)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

### When to Use:
- Track program flow
- Debug issues
- Monitor production systems
- Audit trails

---

## datetime - Date and Time Types

**Purpose:** Work with dates, times, and timestamps

**Official Docs:** https://docs.python.org/3/library/datetime.html

### From Codebase: `api/routers/reports.py`

```python
from datetime import datetime, date

# Convert date object to ISO format string
date_string = row.date_actual.isoformat()  # "2024-01-15"

# Get current timestamp
start_time = datetime.now()  # datetime(2024, 1, 15, 14, 30, 45)

# Get current date
today = date.today()  # date(2024, 1, 15)

# Create specific date
specific_date = date(2024, 1, 15)

# Create specific datetime
specific_datetime = datetime(2024, 1, 15, 14, 30, 0)
```

### Common Methods:

**date objects:**
```python
from datetime import date

d = date(2024, 1, 15)
d.year          # 2024
d.month         # 1
d.day           # 15
d.isoformat()   # "2024-01-15"
d.strftime("%m/%d/%Y")  # "01/15/2024"
```

**datetime objects:**
```python
from datetime import datetime

dt = datetime(2024, 1, 15, 14, 30, 45)
dt.year         # 2024
dt.hour         # 14
dt.minute       # 30
dt.isoformat()  # "2024-01-15T14:30:45"
dt.strftime("%Y-%m-%d %H:%M:%S")  # "2024-01-15 14:30:45"
```

**Time differences:**
```python
from datetime import datetime, timedelta

start = datetime.now()
# ... do some work ...
end = datetime.now()
duration = end - start  # timedelta object

# Add/subtract time
tomorrow = datetime.now() + timedelta(days=1)
last_week = datetime.now() - timedelta(weeks=1)
```

### When to Use:
- Store timestamps
- Calculate time differences
- Format dates for display
- Parse date strings

---

## time - Time Access and Conversions

**Purpose:** Time-related functions (delays, timing, etc.)

**Official Docs:** https://docs.python.org/3/library/time.html

### From Codebase: `etl_pipeline/core/connections.py`

```python
import time

# Wait/sleep for seconds
time.sleep(5)  # Pause for 5 seconds

# Get current time as seconds since epoch
timestamp = time.time()  # 1705329045.123456

# Measure execution time
start = time.time()
# ... do some work ...
elapsed = time.time() - start
print(f"Took {elapsed:.2f} seconds")
```

### Common Uses:
- Add delays between operations
- Retry logic with backoff
- Performance timing
- Rate limiting

### time vs datetime:
- `time` - Low-level Unix timestamps, sleep/delays
- `datetime` - High-level dates/times, formatting, arithmetic

---

## enum - Enumeration Support

**Purpose:** Create named constants with type safety

**Official Docs:** https://docs.python.org/3/library/enum.html

### From Codebase: `api/config.py`

```python
from enum import Enum

class Environment(Enum):
    """Supported API environments."""
    TEST = "test"
    PRODUCTION = "production"

class DatabaseType(Enum):
    """Database types for API connections."""
    ANALYTICS = "analytics"

# Usage
current_env = Environment.TEST
print(current_env.value)  # "test"
print(current_env.name)   # "TEST"

# Iterate over enum
for env in Environment:
    print(env.name, env.value)

# Compare enums
if current_env == Environment.TEST:
    print("Running in test mode")
```

### Why Use Enums:
✅ **Type Safety** - Can't accidentally use wrong value
```python
# Without enum - typo not caught
env = "produktion"  # Oops!

# With enum - error at runtime
env = Environment.PRODUKTION  # AttributeError
```

✅ **Autocomplete** - IDE suggests valid options

✅ **Documentation** - Clear set of valid values

### Common Pattern:
```python
class Status(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"

# Use in functions
def process_task(status: Status):
    if status == Status.COMPLETED:
        # Do something
        pass
```

---

## typing - Type Hints

**Purpose:** Add type annotations for better code documentation and IDE support

**Official Docs:** https://docs.python.org/3/library/typing.html

### From Codebase: `api/config.py`, `api/routers/reports.py`

```python
from typing import Dict, Optional, List, Any

# Type hint function parameters and return value
def get_config() -> Dict[str, str]:
    return {"key": "value"}

# Optional type (can be value or None)
def process_data(start_date: Optional[date] = None) -> List[dict]:
    if start_date:
        # Use start_date
        pass
    return []

# Multiple types
from typing import Union
def get_value() -> Union[int, str]:
    return "hello"  # or return 42
```

### Common Type Hints:

```python
from typing import List, Dict, Tuple, Set, Optional, Any, Union

# Basic types
name: str = "Alice"
age: int = 30
price: float = 19.99
is_active: bool = True

# Collections
numbers: List[int] = [1, 2, 3]
user: Dict[str, Any] = {"name": "Alice", "age": 30}
coordinates: Tuple[float, float] = (40.7128, -74.0060)
tags: Set[str] = {"python", "database", "api"}

# Optional (can be None)
middle_name: Optional[str] = None  # Same as Union[str, None]

# Union (multiple types)
identifier: Union[int, str] = "abc123"

# Function signatures
def greet(name: str, age: int) -> str:
    return f"Hello {name}, you are {age}"

# No return value
def log_message(msg: str) -> None:
    print(msg)
```

### Benefits:
- **IDE Support** - Better autocomplete and error detection
- **Documentation** - Clearly shows what types are expected
- **Static Analysis** - Tools like mypy can catch type errors

**Note:** Type hints are **optional** and don't affect runtime behavior!

---

## String Methods

**Purpose:** Manipulate and transform text

**Official Docs:** https://docs.python.org/3/library/stdtypes.html#string-methods

### From Codebase: `api/config.py`

```python
# Split string into list
"host1,host2,host3".split(',')  # ['host1', 'host2', 'host3']

# Remove whitespace
"  hello  ".strip()              # "hello"
"  hello  ".lstrip()             # "hello  " (left strip)
"  hello  ".rstrip()             # "  hello" (right strip)

# Change case
"API_ENVIRONMENT".lower()        # "api_environment"
"debug".upper()                  # "DEBUG"
"hello world".title()            # "Hello World"

# Replace characters
"api.cors.origins".replace('.', '_')  # "api_cors_origins"

# Check contents
"hello".startswith('he')         # True
"world.txt".endswith('.txt')     # True
"abc123".isdigit()               # False
"123".isdigit()                  # True
"hello".isalpha()                # True

# Join list into string
','.join(['a', 'b', 'c'])        # "a,b,c"
' '.join(['Hello', 'World'])     # "Hello World"
```

### Common Patterns:

```python
# Parse CSV-like data
cors_origins = "http://localhost:3000, http://localhost:5173"
origins = [origin.strip() for origin in cors_origins.split(',')]
# ['http://localhost:3000', 'http://localhost:5173']

# Build SQL WHERE clause
conditions = []
if name:
    conditions.append("name = :name")
if age:
    conditions.append("age = :age")
where_clause = " AND ".join(conditions)
# "name = :name AND age = :age"

# Normalize user input
user_input = "  TeSt  "
normalized = user_input.strip().lower()  # "test"
```

---

## Type Conversions

**Purpose:** Convert between data types

### From Codebase: `api/config.py`, `api/routers/reports.py`

```python
# String to integer
port = int("5432")               # 5432
age = int("30")                  # 30

# String to float
price = float("19.99")           # 19.99
rate = float("0.05")             # 0.05

# To string
str(5432)                        # "5432"
str(19.99)                       # "19.99"

# Handle None/null values
float(row.total_revenue or 0)    # 0 if total_revenue is None

# Boolean conversion
bool("true")                     # True (non-empty string)
bool("")                         # False (empty string)
bool(0)                          # False
bool(42)                         # True
```

### Common Patterns:

```python
# Safe integer conversion
try:
    port = int(value)
except ValueError:
    port = 5432  # Default value

# Convert with default
amount = float(row.amount or 0)

# Parse boolean from string
debug_str = os.getenv('DEBUG', 'false')
debug = debug_str.lower() == 'true'
```

---

## Built-in Functions

### len() - Get Length

```python
# String length
len("hello")                     # 5

# List length
len([1, 2, 3, 4])               # 4

# Dictionary size
len({"a": 1, "b": 2})           # 2

# Check if empty
if len(items) > 0:
    # Has items

# Pythonic way
if items:
    # Has items (preferred)
```

### range() - Generate Number Sequences

```python
# 0 to 9
for i in range(10):
    print(i)

# 1 to 10
for i in range(1, 11):
    print(i)

# Step by 2
for i in range(0, 10, 2):
    print(i)  # 0, 2, 4, 6, 8

# Create list
numbers = list(range(5))  # [0, 1, 2, 3, 4]
```

### enumerate() - Loop with Index

```python
items = ['a', 'b', 'c']

for index, value in enumerate(items):
    print(f"{index}: {value}")
# 0: a
# 1: b
# 2: c

# Start from 1
for index, value in enumerate(items, start=1):
    print(f"{index}: {value}")
```

### zip() - Combine Iterables

```python
names = ['Alice', 'Bob', 'Charlie']
ages = [25, 30, 35]

for name, age in zip(names, ages):
    print(f"{name} is {age}")
# Alice is 25
# Bob is 30
# Charlie is 35
```

---

## List Comprehensions

**Purpose:** Create lists in a concise, readable way

### From Codebase: `api/routers/reports.py`

```python
# Basic list comprehension
result = db.execute(text(query), params).fetchall()

return [
    {
        "date": row.date_actual.isoformat(),
        "revenue_lost": float(row.total_revenue_lost or 0),
        "opportunity_count": row.opportunity_count
    }
    for row in result
]
```

### Syntax Breakdown:

```python
# Pattern: [expression for item in iterable if condition]

# Simple transformation
numbers = [1, 2, 3, 4, 5]
squares = [n**2 for n in numbers]
# [1, 4, 9, 16, 25]

# With condition
evens = [n for n in numbers if n % 2 == 0]
# [2, 4]

# String processing
names = ["  Alice  ", "  Bob  "]
clean_names = [name.strip() for name in names]
# ["Alice", "Bob"]

# Transform and filter
prices = ["$10.99", "$25.50", "$5.00"]
floats = [float(p.replace('$', '')) for p in prices if float(p.replace('$', '')) > 10]
# [10.99, 25.50]
```

### Equivalent For Loop:

```python
# List comprehension
squares = [n**2 for n in numbers]

# Equivalent for loop
squares = []
for n in numbers:
    squares.append(n**2)
```

### When to Use:
✅ Simple transformations
✅ Creating lists from other iterables
❌ Complex logic (use regular for loop instead)

---

## Dictionary Operations

**Purpose:** Work with key-value data structures

### From Codebase: `api/routers/reports.py`

```python
# Create empty dictionary
params = {}

# Add key-value pairs
params['start_date'] = start_date
params['end_date'] = end_date

# Get value (returns None if not found)
value = params.get('provider_id')

# Get with default
port = config.get('port', 5432)

# Check if key exists
if 'start_date' in params:
    # Key exists

# Iterate over dictionary
for key, value in params.items():
    print(f"{key}: {value}")

# Get all keys
keys = params.keys()

# Get all values
values = params.values()
```

### Common Patterns:

```python
# Build dictionary conditionally
params = {}
if start_date:
    params['start_date'] = start_date
if end_date:
    params['end_date'] = end_date

# Dictionary comprehension
squares = {n: n**2 for n in range(5)}
# {0: 0, 1: 1, 2: 4, 3: 9, 4: 16}

# Merge dictionaries (Python 3.9+)
dict1 = {'a': 1, 'b': 2}
dict2 = {'c': 3, 'd': 4}
merged = dict1 | dict2
# {'a': 1, 'b': 2, 'c': 3, 'd': 4}

# Unpack dictionaries
config = {**default_config, **user_config}
```

---

## Error Handling

**Purpose:** Handle exceptions gracefully

### From Codebase: `api/database.py`, `api/config.py`

```python
# Basic try-except
try:
    port = int(value)
except ValueError:
    port = 5432  # Default value

# Try-except-finally
try:
    db = SessionLocal()
    yield db
except Exception as e:
    logger.error(f"Database error: {e}")
    raise
finally:
    db.close()  # Always runs, even if exception

# Multiple exceptions
try:
    result = risky_operation()
except ValueError as e:
    logger.error(f"Invalid value: {e}")
except ConnectionError as e:
    logger.error(f"Connection failed: {e}")
except Exception as e:
    logger.error(f"Unexpected error: {e}")

# Re-raise exception
try:
    dangerous_operation()
except Exception as e:
    logger.error("Operation failed")
    raise  # Re-raise the same exception
```

### Common Pattern:

```python
# Resource cleanup pattern
try:
    # Acquire resource
    connection = get_connection()
    
    # Use resource
    result = connection.execute(query)
    
    return result
except Exception as e:
    # Handle error
    logger.exception("Query failed")
    raise
finally:
    # Always cleanup
    connection.close()
```

---

## String Formatting

**Purpose:** Build strings with variables

### From Codebase: `api/config.py`

### f-strings (Python 3.6+) - **Recommended**

```python
# Basic interpolation
name = "Alice"
age = 30
message = f"Hello {name}, you are {age}"
# "Hello Alice, you are 30"

# Expressions inside {}
price = 19.99
tax_rate = 0.08
total = f"Total: ${price * (1 + tax_rate):.2f}"
# "Total: $21.59"

# Database connection string
user = "postgres"
password = "secret"
host = "localhost"
port = 5432
database = "mydb"

url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
# "postgresql://postgres:secret@localhost:5432/mydb"

# Formatting numbers
value = 1234.5678
f"{value:.2f}"    # "1234.57" (2 decimal places)
f"{value:,.2f}"   # "1,234.57" (thousands separator)
f"{value:>10}"    # "  1234.5678" (right-aligned, width 10)
```

### .format() Method (Older Style)

```python
"Hello {}, you are {}".format(name, age)
"Hello {name}, you are {age}".format(name="Alice", age=30)
```

### % Operator (Legacy)

```python
"Hello %s, you are %d" % (name, age)
```

**Use f-strings** - They're faster, more readable, and the modern standard!

---

## Context Managers

**Purpose:** Automatic resource management (setup/cleanup)

### The `with` Statement

```python
# File handling
with open('data.txt', 'r') as f:
    content = f.read()
    # File automatically closed when exiting block

# Multiple resources
with open('input.txt', 'r') as infile, open('output.txt', 'w') as outfile:
    data = infile.read()
    outfile.write(data.upper())

# Database connections
with engine.connect() as connection:
    result = connection.execute(query)
    # Connection automatically closed
```

### Why Use Context Managers:

**Without context manager:**
```python
f = open('data.txt', 'r')
try:
    content = f.read()
finally:
    f.close()  # Must remember to close
```

**With context manager:**
```python
with open('data.txt', 'r') as f:
    content = f.read()
# Automatically closed, even if exception occurs
```

### Benefits:
✅ Automatic cleanup
✅ Exception-safe
✅ Cleaner code
✅ Prevents resource leaks

### Common Uses:
- File operations
- Database connections
- Locks and semaphores
- Temporary directories

---

## Global Variables

**Purpose:** Module-level state management

### From Codebase: `api/config.py`

```python
# Module-level variable
_global_config = None

def get_config() -> APIConfig:
    """Get global configuration instance (lazy initialization)."""
    global _global_config  # Declare we're modifying global variable
    
    if _global_config is None:
        _global_config = APIConfig()
    
    return _global_config

def reset_config():
    """Reset global configuration (for testing)."""
    global _global_config
    _global_config = None
```

### Global Statement:

```python
counter = 0

def increment():
    global counter  # Required to modify global variable
    counter += 1

increment()
print(counter)  # 1
```

### When to Use:
- Singleton patterns
- Application configuration
- Caches
- Connection pools

### Caution:
⚠️ Use sparingly - can make code harder to test and reason about
✅ Consider dependency injection instead
✅ Document clearly when globals are necessary

---

## Quick Reference Table

### System & Environment
| Task | Module/Method | Example |
|------|--------------|---------|
| Environment variable | `os.getenv()` | `os.getenv('API_KEY')` |
| File paths | `pathlib.Path` | `Path(__file__).parent` |
| Command line args | `sys.argv` | `sys.argv[1]` |
| Exit program | `sys.exit()` | `sys.exit(1)` |

### Logging & Time
| Task | Module/Method | Example |
|------|--------------|---------|
| Logging | `logging.getLogger()` | `logger.info("message")` |
| Current time | `datetime.now()` | `datetime.now()` |
| Date to string | `.isoformat()` | `date.today().isoformat()` |
| Wait/delay | `time.sleep()` | `time.sleep(5)` |

### Data Types & Structures
| Task | Module/Method | Example |
|------|--------------|---------|
| Constants | `enum.Enum` | `class Status(Enum):` |
| Type hints | `typing` | `def func() -> List[str]:` |
| Default dict | `defaultdict` | `defaultdict(list)` |
| Count items | `Counter` | `Counter(['a','b','a'])` |
| Named tuple | `namedtuple` | `Point = namedtuple('Point', ['x', 'y'])` |
| Precise decimal | `Decimal` | `Decimal('19.99')` |
| Data class | `@dataclass` | `@dataclass\nclass Point:` |

### Data Formats
| Task | Module/Method | Example |
|------|--------------|---------|
| Parse JSON | `json.loads()` | `json.loads('{"key": "value"}')` |
| Write JSON | `json.dumps()` | `json.dumps(data, indent=2)` |
| Read YAML | `yaml.safe_load()` | `yaml.safe_load(file)` |

### Randomness
| Task | Module/Method | Example |
|------|--------------|---------|
| Random choice | `random.choice()` | `random.choice([1,2,3])` |
| Random int | `random.randint()` | `random.randint(1, 100)` |
| Random float | `random.random()` | `random.random()` |
| Weighted choice | `random.choices()` | `random.choices([1,2], weights=[0.7, 0.3])` |

### Functions & Decorators
| Task | Module/Method | Example |
|------|--------------|---------|
| Cache results | `@lru_cache` | `@lru_cache(maxsize=128)` |
| Preserve metadata | `@wraps` | `@wraps(func)` |
| Partial function | `partial()` | `double = partial(multiply, 2)` |

### Strings
| Task | Module/Method | Example |
|------|--------------|---------|
| Split string | `.split()` | `"a,b,c".split(',')` |
| Remove whitespace | `.strip()` | `"  text  ".strip()` |
| Case conversion | `.lower()`, `.upper()` | `"TEXT".lower()` |
| String with vars | f-string | `f"Hello {name}"` |
| Join list | `.join()` | `','.join(['a','b'])` |

### Type Conversions
| Task | Module/Method | Example |
|------|--------------|---------|
| To integer | `int()` | `int("42")` |
| To float | `float()` | `float("3.14")` |
| To string | `str()` | `str(42)` |

### Collections
| Task | Module/Method | Example |
|------|--------------|---------|
| Length | `len()` | `len([1,2,3])` |
| List comprehension | `[...]` | `[x**2 for x in range(5)]` |
| List slice | `list[start:end]` | `items[2:5]` |
| Unpack list | `*rest` | `first, *rest = [1,2,3]` |
| Empty dict | `{}` | `params = {}` |
| Dict get default | `.get()` | `d.get('key', 'default')` |

### Error Handling
| Task | Module/Method | Example |
|------|--------------|---------|
| Try/except | `try/except` | `try: ... except ValueError:` |
| Auto cleanup | `with` statement | `with open('f') as f:` |
| Global variable | `global` keyword | `global counter` |

### OOP
| Task | Module/Method | Example |
|------|--------------|---------|
| Constructor | `__init__` | `def __init__(self, x):` |
| String repr | `__str__`, `__repr__` | `def __str__(self):` |
| Inherit parent | `super()` | `super().__init__()` |

---

## Learning Resources

### Official Documentation
- **Python Standard Library:** https://docs.python.org/3/library/
- **Python Tutorial:** https://docs.python.org/3/tutorial/

### Books
- "Python Crash Course" by Eric Matthes
- "Fluent Python" by Luciano Ramalho
- "Effective Python" by Brett Slatkin

### Online Resources
- **Real Python:** https://realpython.com/
- **Python.org Tutorials:** https://docs.python.org/3/tutorial/
- **W3Schools Python:** https://www.w3schools.com/python/

### Practice
- **LeetCode:** https://leetcode.com/
- **HackerRank:** https://www.hackerrank.com/domains/python
- **Codewars:** https://www.codewars.com/

---

---

## random - Random Number Generation

**Purpose:** Generate pseudo-random numbers for simulations, testing, and data generation

**Official Docs:** https://docs.python.org/3/library/random.html

### From Codebase: `etl_pipeline/synthetic_data_generator/generators/patient_generator.py`

```python
import random

# Random choice from list
clinic_id = random.choice(self.data_store['clinics'])
provider = random.choice(self.data_store['providers_dentist'])

# Weighted random choice
family_size = random.choices(
    [1, 2, 3, 4, 5],  # Options
    weights=[0.20, 0.30, 0.25, 0.15, 0.10]  # Probabilities
)[0]

# Random number in range
age = random.randint(25, 70)  # 25 to 70 inclusive

# Random float (0.0 to 1.0)
if random.random() < 0.7:  # 70% probability
    # Do something
    pass

# Random integer in range with step
number = random.randrange(0, 100, 5)  # 0, 5, 10, ..., 95

# Shuffle a list in place
items = [1, 2, 3, 4, 5]
random.shuffle(items)  # items is now randomized

# Random sample (without replacement)
selected = random.sample(population, k=3)  # Pick 3 unique items
```

### Common Patterns:

```python
# Generate test data
test_emails = [f"user{random.randint(1, 1000)}@example.com" for _ in range(10)]

# Random boolean with probability
is_active = random.random() < 0.85  # 85% chance of True

# Random date within range
from datetime import datetime, timedelta
random_days = random.randint(0, 365)
random_date = datetime.now() - timedelta(days=random_days)

# Set seed for reproducibility (testing)
random.seed(42)  # Same sequence every time
```

### When to Use:
- Synthetic data generation
- Simulations and Monte Carlo methods
- Testing with random inputs
- Sampling from datasets

---

## json - JSON Encoding and Decoding

**Purpose:** Work with JSON data (JavaScript Object Notation)

**Official Docs:** https://docs.python.org/3/library/json.html

### From Codebase: `etl_pipeline/tests/`

```python
import json

# Parse JSON string to Python object
json_string = '{"name": "Alice", "age": 30}'
data = json.loads(json_string)
# data = {'name': 'Alice', 'age': 30}

# Convert Python object to JSON string
person = {'name': 'Bob', 'age': 25, 'active': True}
json_string = json.dumps(person)
# '{"name": "Bob", "age": 25, "active": true}'

# Pretty print JSON
json_string = json.dumps(person, indent=2)
# {
#   "name": "Bob",
#   "age": 25,
#   "active": true
# }

# Read JSON from file
with open('data.json', 'r') as f:
    data = json.load(f)  # Note: load() not loads()

# Write JSON to file
with open('output.json', 'w') as f:
    json.dump(data, f, indent=2)  # Note: dump() not dumps()

# Handle non-serializable types
from datetime import datetime

def date_handler(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

data = {'timestamp': datetime.now()}
json_string = json.dumps(data, default=date_handler)
```

### Common Patterns:

```python
# API response formatting
response = {
    'status': 'success',
    'data': results,
    'count': len(results)
}
return json.dumps(response)

# Configuration files
config = {
    'database': 'mydb',
    'host': 'localhost',
    'port': 5432
}
with open('config.json', 'w') as f:
    json.dump(config, f, indent=2)

# Validate JSON structure
try:
    data = json.loads(json_string)
except json.JSONDecodeError as e:
    print(f"Invalid JSON: {e}")
```

---

## yaml - YAML Parser

**Purpose:** Read and write YAML configuration files

**Official Docs:** https://pyyaml.org/wiki/PyYAMLDocumentation

**Note:** Part of PyYAML external library, but commonly used like standard library

### From Codebase: `etl_pipeline/etl_pipeline/config/config_reader.py`

```python
import yaml

# Read YAML file
with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)  # safe_load prevents code execution

# Write YAML file
data = {
    'database': {
        'host': 'localhost',
        'port': 5432,
        'name': 'mydb'
    },
    'tables': ['users', 'orders', 'products']
}

with open('output.yml', 'w') as f:
    yaml.dump(data, f, default_flow_style=False)

# Handle YAML errors
try:
    with open('config.yml', 'r') as f:
        config = yaml.safe_load(f)
except yaml.YAMLError as e:
    logger.error(f"YAML parsing error: {e}")
```

### YAML vs JSON:

```yaml
# YAML (more human-readable)
database:
  host: localhost
  port: 5432
  tables:
    - users
    - orders
    - products

# Equivalent JSON
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "tables": ["users", "orders", "products"]
  }
}
```

---

## decimal - Decimal Fixed Point Arithmetic

**Purpose:** Precise decimal arithmetic (avoid floating point errors)

**Official Docs:** https://docs.python.org/3/library/decimal.html

### From Codebase: `etl_pipeline/synthetic_data_generator/`

```python
from decimal import Decimal

# Avoid floating point precision errors
price = Decimal('19.99')  # Use string, not float!
tax_rate = Decimal('0.08')
total = price * (1 + tax_rate)
# Decimal('21.5892')

# Precise financial calculations
amount = Decimal('100.00')
discount = Decimal('0.15')
final = amount * (Decimal('1') - discount)
# Decimal('85.00')

# Set precision
from decimal import getcontext
getcontext().prec = 2  # 2 decimal places

# Round properly
amount = Decimal('10.555')
rounded = amount.quantize(Decimal('0.01'))  # 10.56

# Compare decimals safely
price1 = Decimal('19.99')
price2 = Decimal('19.99')
price1 == price2  # True (unlike floats!)
```

### Why Use Decimal:

```python
# Float precision issues
0.1 + 0.2  # 0.30000000000000004 ❌

# Decimal precision
Decimal('0.1') + Decimal('0.2')  # Decimal('0.3') ✅

# Financial calculations
# NEVER use float for money!
float_total = 0.1 + 0.1 + 0.1  # 0.30000000000000004
decimal_total = Decimal('0.1') + Decimal('0.1') + Decimal('0.1')  # Decimal('0.3')
```

### When to Use:
- Financial calculations (money, prices)
- Scientific calculations requiring precision
- Any calculation where precision matters
- Avoiding floating point errors

---

## collections - Specialized Container Types

**Purpose:** Additional data structures beyond list, dict, set, tuple

**Official Docs:** https://docs.python.org/3/library/collections.html

### From Codebase: `etl_pipeline/scripts/analyze_connection_usage.py`

### defaultdict - Dictionary with Default Values

```python
from collections import defaultdict

# Count occurrences
word_count = defaultdict(int)  # Default value is 0
for word in words:
    word_count[word] += 1  # No KeyError!

# Group items
from collections import defaultdict
students_by_grade = defaultdict(list)  # Default value is []
students_by_grade['A'].append('Alice')  # No KeyError!
students_by_grade['B'].append('Bob')

# Nested dictionaries
nested = defaultdict(lambda: defaultdict(int))
nested['category']['item'] += 1  # Works!
```

### Counter - Count Hashable Objects

```python
from collections import Counter

# Count items in list
items = ['apple', 'banana', 'apple', 'orange', 'banana', 'apple']
counts = Counter(items)
# Counter({'apple': 3, 'banana': 2, 'orange': 1})

# Most common items
counts.most_common(2)  # [('apple', 3), ('banana', 2)]

# Combine counters
counter1 = Counter(['a', 'b', 'c'])
counter2 = Counter(['b', 'c', 'd'])
combined = counter1 + counter2  # Counter({'b': 2, 'c': 2, 'a': 1, 'd': 1})

# Count from string
letter_count = Counter("hello world")
# Counter({'l': 3, 'o': 2, 'h': 1, 'e': 1, ' ': 1, 'w': 1, 'r': 1, 'd': 1})
```

### namedtuple - Lightweight Objects

```python
from collections import namedtuple

# Define a simple class
Point = namedtuple('Point', ['x', 'y'])
p = Point(10, 20)
print(p.x, p.y)  # 10 20

# Database rows
Row = namedtuple('Row', ['id', 'name', 'age'])
user = Row(1, 'Alice', 30)
print(user.name)  # Alice

# More readable than tuples
# Instead of: person = (1, 'Alice', 30)
Person = namedtuple('Person', ['id', 'name', 'age'])
person = Person(1, 'Alice', 30)
print(person.name)  # Much clearer!
```

---

## functools - Higher-order Functions

**Purpose:** Tools for working with functions (decorators, caching, etc.)

**Official Docs:** https://docs.python.org/3/library/functools.html

### From Codebase: `etl_pipeline/tests/fixtures/mock_utils.py`

### wraps - Preserve Function Metadata in Decorators

```python
from functools import wraps

def my_decorator(func):
    @wraps(func)  # Preserves func's name and docstring
    def wrapper(*args, **kwargs):
        print(f"Calling {func.__name__}")
        return func(*args, **kwargs)
    return wrapper

@my_decorator
def greet(name):
    """Say hello to someone."""
    return f"Hello {name}"

# Without @wraps, greet.__name__ would be 'wrapper'
print(greet.__name__)  # 'greet' ✅
print(greet.__doc__)   # 'Say hello to someone.' ✅
```

### lru_cache - Memoization/Caching

```python
from functools import lru_cache

@lru_cache(maxsize=128)  # Cache up to 128 results
def fibonacci(n):
    if n < 2:
        return n
    return fibonacci(n-1) + fibonacci(n-2)

# Much faster for repeated calls
fibonacci(100)  # Computed once, then cached

# Check cache info
fibonacci.cache_info()  # hits, misses, size
fibonacci.cache_clear()  # Clear the cache
```

### partial - Partial Function Application

```python
from functools import partial

def multiply(x, y):
    return x * y

# Create specialized version
double = partial(multiply, 2)
triple = partial(multiply, 3)

double(5)  # 10
triple(5)  # 15

# Useful for callbacks
from functools import partial
button.on_click(partial(save_data, database=db))
```

---

## dataclasses - Data Classes

**Purpose:** Automatically generate special methods for classes

**Official Docs:** https://docs.python.org/3/library/dataclasses.html

```python
from dataclasses import dataclass
from typing import Optional

# Traditional class (verbose)
class PersonOld:
    def __init__(self, name: str, age: int, email: Optional[str] = None):
        self.name = name
        self.age = age
        self.email = email
    
    def __repr__(self):
        return f"Person(name={self.name}, age={self.age}, email={self.email})"
    
    def __eq__(self, other):
        return (self.name, self.age, self.email) == (other.name, other.age, other.email)

# Data class (concise) - same functionality!
@dataclass
class Person:
    name: str
    age: int
    email: Optional[str] = None

# Automatically generated:
p = Person("Alice", 30)
print(p)  # Person(name='Alice', age=30, email=None)
p1 == p2  # Comparison works automatically

# Additional features
@dataclass(frozen=True)  # Make immutable
class Point:
    x: int
    y: int

@dataclass(order=True)  # Enable <, >, etc.
class Student:
    grade: int
    name: str
```

---

## Pydantic - Data Validation

**Purpose:** Data validation using Python type hints

**Official Docs:** https://docs.pydantic.dev/

**Note:** External library, but commonly used in modern Python APIs

### From Codebase: `api/models/patient.py`, `api/api_types.py`

```python
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class Patient(BaseModel):
    # Type validation happens automatically!
    patient_id: int
    name: str
    age: Optional[int] = None
    birth_date: Optional[datetime] = None
    balance: Optional[float] = None
    
    class Config:
        from_attributes = True  # Allow ORM models

# Validation
patient = Patient(patient_id="1", name="Alice")  # "1" converted to 1
# patient = Patient(patient_id="abc")  # ValidationError!

# Nested models
class Address(BaseModel):
    street: str
    city: str
    zipcode: str

class Patient(BaseModel):
    name: str
    address: Address  # Nested validation

# List validation
class PatientList(BaseModel):
    patients: List[Patient]
    total: int
```

---

## Special Methods (Dunder Methods)

**Purpose:** Define custom behavior for Python objects

**Official Docs:** https://docs.python.org/3/reference/datamodel.html

### From Codebase: `etl_pipeline/etl_pipeline/exceptions/base.py`

```python
class ETLException(Exception):
    def __init__(self, message, table_name=None):
        """Called when creating instance"""
        self.message = message
        self.table_name = table_name
        super().__init__(message)
    
    def __str__(self):
        """Called by str() and print()"""
        return f"{self.message} | Table: {self.table_name}"
    
    def __repr__(self):
        """Called by repr() - for debugging"""
        return (
            f"ETLException("
            f"message='{self.message}', "
            f"table_name='{self.table_name}')"
        )
```

### Common Special Methods:

```python
class Point:
    def __init__(self, x, y):
        """Constructor"""
        self.x = x
        self.y = y
    
    def __str__(self):
        """Human-readable string"""
        return f"Point({self.x}, {self.y})"
    
    def __repr__(self):
        """Developer-friendly representation"""
        return f"Point(x={self.x}, y={self.y})"
    
    def __eq__(self, other):
        """Equality comparison (==)"""
        return self.x == other.x and self.y == other.y
    
    def __lt__(self, other):
        """Less than (<)"""
        return (self.x, self.y) < (other.x, other.y)
    
    def __len__(self):
        """Length"""
        return 2
    
    def __getitem__(self, index):
        """Index access (point[0])"""
        return [self.x, self.y][index]
    
    def __add__(self, other):
        """Addition (+)"""
        return Point(self.x + other.x, self.y + other.y)
    
    def __enter__(self):
        """Context manager enter"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        pass
```

---

## Advanced List Operations

### List Slicing

```python
items = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

# Basic slicing [start:stop:step]
items[2:5]      # [2, 3, 4]
items[:3]       # [0, 1, 2] (first 3)
items[7:]       # [7, 8, 9] (last 3)
items[-3:]      # [7, 8, 9] (last 3)
items[::2]      # [0, 2, 4, 6, 8] (every 2nd)
items[::-1]     # [9, 8, 7, ...] (reverse)

# Modify with slicing
items[2:5] = [20, 30, 40]  # Replace elements
items[::2] = [0] * 5        # Replace every 2nd element
```

### List Unpacking

```python
# Unpack into variables
a, b, c = [1, 2, 3]

# Rest operator
first, *rest = [1, 2, 3, 4, 5]
# first = 1, rest = [2, 3, 4, 5]

first, *middle, last = [1, 2, 3, 4, 5]
# first = 1, middle = [2, 3, 4], last = 5

# Ignore values
first, _, third = [1, 2, 3]  # _ is convention for unused

# Swap values
a, b = b, a  # Elegant swap!

# Function arguments
def process(x, y, z):
    return x + y + z

values = [1, 2, 3]
result = process(*values)  # Unpack list as arguments
```

---

## Class Inheritance and super()

### From Codebase: `etl_pipeline/etl_pipeline/exceptions/`

```python
class ETLException(Exception):
    """Base exception class"""
    def __init__(self, message, table_name=None):
        self.message = message
        self.table_name = table_name
        super().__init__(message)  # Call parent __init__

class DatabaseConnectionError(ETLException):
    """Specific exception type"""
    def __init__(self, message, host=None, port=None):
        super().__init__(message)  # Call parent __init__
        self.host = host
        self.port = port

# Usage
try:
    raise DatabaseConnectionError(
        "Connection failed",
        host="localhost",
        port=5432
    )
except DatabaseConnectionError as e:
    print(e.message)  # From ETLException
    print(e.host)     # From DatabaseConnectionError
except ETLException as e:
    # Catches all ETL exceptions
    print(e)
```

### Inheritance Patterns:

```python
class Animal:
    def __init__(self, name):
        self.name = name
    
    def speak(self):
        raise NotImplementedError("Subclass must implement")

class Dog(Animal):
    def speak(self):
        return f"{self.name} says Woof!"

class Cat(Animal):
    def speak(self):
        return f"{self.name} says Meow!"

# Multiple inheritance
class Manager:
    def manage(self):
        return "Managing..."

class Engineer:
    def code(self):
        return "Coding..."

class TechLead(Manager, Engineer):
    """Inherits from both"""
    pass

lead = TechLead()
lead.manage()  # Works!
lead.code()    # Works!
```

---

## sys - System-specific Parameters

**Purpose:** Access system-specific parameters and functions

**Official Docs:** https://docs.python.org/3/library/sys.html

```python
import sys

# Command line arguments
# python script.py arg1 arg2
print(sys.argv)  # ['script.py', 'arg1', 'arg2']

# Exit program with status code
sys.exit(0)  # Success
sys.exit(1)  # Error

# Python version
print(sys.version)
print(sys.version_info)  # (3, 11, 0, 'final', 0)

# Module search path
sys.path.append('/custom/path')

# Standard streams
sys.stdout.write("Output\n")
sys.stderr.write("Error\n")
sys.stdin.readline()

# Platform information
print(sys.platform)  # 'win32', 'linux', 'darwin'

# Maximum integer size
print(sys.maxsize)
```

---

## Next Steps

1. **Read through each section** - Don't try to memorize everything
2. **Experiment in Python REPL** - Type `python` in terminal and try examples
3. **Read official docs** - Click through to understand each module deeply
4. **Look for patterns in code** - See how these are used in the codebase
5. **Practice regularly** - Use these in your own scripts

Remember: You don't need to memorize everything. Know what's available, and look up details when needed!

