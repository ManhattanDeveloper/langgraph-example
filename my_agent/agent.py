from langchain_core.messages import SystemMessage
from langchain_openai import ChatOpenAI

from langgraph.graph import START, StateGraph, MessagesState
from langgraph.prebuilt import tools_condition, ToolNode

from pymongo import MongoClient
import json
from bson.json_util import dumps

all_schemas = [
        {
            "collection": "courses",
            "fields": {
                "id": {
                    "type": "string",
                    "description": "Unique ID"
                },
                "version": {
                    "type": "number", 
                    "description": "Course's version number"
                },
                "lastEdited": {
                    "type": "integer",
                    "description": "Last time the course was edited, as a unix timestamp"
                },
                "lastEditedBy": {
                    "type": "string",
                    "description": "Last person to edit this course"
                },
                "rolloverSetting": {
                    "type": "string",
                    "description": "Course's rollover setting"
                },
                "institutionId": {
                    "type": "string",
                    "description": "Institution's unique identifier for this course"
                },
                "code": {
                    "type": "string",
                    "description": "Course's code, such as 'BIO100'"
                },
                "subjectCode": {
                    "type": "string",
                    "description": "Course's subject code, such as 'BIO'"
                },
                "courseNumber": {
                    "type": "string",
                    "description": "Course's course number, such as '100'"
                },
                "name": {
                    "type": "string",
                    "description": "Course's name"
                },
                "description": {
                    "type": "string",
                    "description": "Course's description"
                },
                "status": {
                    "type": "string",
                    "description": "Course's status"
                },
                "departments": {
                    "type": "array",
                    "description": "List of course's departments"
                },
                "courseAttributes": {
                    "type": "array",
                    "description": "List of attributes of the course"
                },
                "courseNotes": {
                    "type": "string",
                    "description": "Any notes on the course"
                },
                "college": {
                    "type": "string",
                    "description": "The college this course is in"
                },
                "careerCode": {
                    "type": "string",
                    "description": "SIS, this course's career code"
                },
                "catalogNumber": {
                    "type": "string", 
                    "description": "SIS, this course's catalog number"
                },
                "sections": {
                    "type": "object",
                    "description": "Object containing all section objects, keyed by section ID"
                },
            }
        },
        {
            "collection": "buildings",
            "fields": {
                "id": {
                    "type": "string",
                    "description": "Unique identifier for the building"
                },
                "name": {
                    "type": "string",
                    "description": "Building's name"
                },
                "displayName": {
                    "type": "string", 
                    "description": "Building's display name"
                },
                "description": {
                    "type": "string",
                    "description": "Building's description"
                },
                "departments": {
                    "type": "array",
                    "description": "List of departments using the building",
                },
                "addressLine1": {
                    "type": "string",
                    "description": "Building's address line 1"
                },
                "addressLine2": {
                    "type": "string",
                    "description": "Building's address line 2"
                },
                "city": {
                    "type": "string",
                    "description": "Building's city"
                },
                "state": {
                    "type": "string",
                    "description": "Building's state"
                },
                "zipcode": {
                    "type": "string",
                    "description": "Building's zipcode"
                },
                "notes": {
                    "type": "string",
                    "description": "Notes on the building"
                },
                "availableTimes": {
                    "type": "array",
                    "description": "List of times when the building is available"
                },
                "blockedOutTimes": {
                    "type": "array",
                    "description": "List of times when the building is unavailable"
                },
                "blackoutDates": {
                    "type": "array",
                    "description": "List of dates when the room is in blackout"
                }
            }
        },
        {
            "collection": "rooms",
            "fields": {
                "id": {
                    "type": "string",
                    "description": "Unique identifier for the room"
                },
                "name": {
                    "type": "string", 
                    "description": "Room's name"
                },
                "roomNumber": {
                    "type": "string",
                    "description": "Room's number"
                },
                "displayName": {
                    "type": "string",
                    "description": "Room's display name"
                },
                "buildingId": {
                    "type": "string",
                    "description": "Reference to the building ID"
                },
                "buildingDisplayName": {
                    "type": "string",
                    "description": "Display name of the associated building"
                },
                "campus": {
                    "type": "string",
                    "description": "Room's campus"
                },
                "floor": {
                    "type": "string",
                    "description": "Room's floor"
                },
                "capacity": {
                    "type": "integer",
                    "description": "Number of seats available in the room"
                },
                "minCapacity": {
                    "type": "integer",
                    "description": "Minimum number of seats that should be occupied"
                },
                "departments": {
                    "type": "array",
                    "description": "List of departments using the room"
                },
                "features": {
                    "type": "array",
                    "description": "List of room's features"
                },
                "status": {
                    "type": "string",
                    "description": "Room's status"
                },
                "online": {
                    "type": "boolean",
                    "description": "Whether this room is for online courses"
                },
                "customFields": {
                    "type": "object",
                    "description": "Map of institution-specific fields"
                }
            }
        },
        {
            "collection": "professors",
            "fields": {
                "id": {
                    "type": "string",
                    "description": "Unique identifier for the faculty member"
                },
                "firstName": {
                    "type": "string", 
                    "description": "Professor's first name"
                },
                "lastName": {
                    "type": "string",
                    "description": "Professor's last name"
                },
                "bio": {
                    "type": "string",
                    "description": "Professor's biography"
                },
                "type": {
                    "type": "string",
                    "description": "Professor's type"
                },
                "email": {
                    "type": "string",
                    "description": "Professor's email"
                },
                "departments": {
                    "type": "array",
                    "description": "Professor's departments"
                },
                "status": {
                    "type": "string",
                    "description": "Professor's status"
                },
                "institutionId": {
                    "type": "string",
                    "description": "Professor's institution-specific ID"
                },
                "optimizerPriority": {
                    "type": "number",
                    "description": "A priority rating used by the section optimizer"
                }
            }
        },
        {
            "collection": "departments",
            "fields": {
                "id": {
                    "type": "string",
                    "description": "Unique identifier for the department"
                },
                "name": {
                    "type": "string", 
                    "description": "Department's name"
                },
                "displayName": {
                    "type": "string",
                    "description": "Department's display name"
                },
                "subjectCodes": {
                    "type": "string",
                    "description": "List of associated subject codes, such as 'BIO'"
                },
                "status": {
                    "type": "string",
                    "description": "Department's status"
                },
                "chair": {
                    "type": "array",
                    "description": "Department's chairs"
                },
                "workflowStep": {
                    "type": "object",
                    "description": "Associated workflow step to this department"
                },
                "scheduleStatus": {
                    "type": "object",
                    "description": "Status of the scheduler for this department, keyed by year then semester"
                },
                "preferenceTypeOptions": {
                    "type": "object",
                    "description": "Department's preferences"
                }
            }
        }
    ]
    

def add(a: int, b: int) -> int:
    """Adds a and b.

    Args:
        a: first int
        b: second int
    """
    return a + b

def identify_relevant_mongodb_collections(user_query: str) -> str:
    """Takes the user query and identifies the relevant MongoDB collections.

    Args:
        user_query: user query
    """
    gpt4o_chat = ChatOpenAI(model="gpt-4o", temperature=0)
    return gpt4o_chat.invoke(f"""You will be given a query from a user. Your job is to figure out which collections in MongoDB are relevant to the query. 
                             Return only the collections that are relevant to the query as a comma separated list of collection names: `professors, courses, departments`. 
                             For example, if the user asks about professors and courses, you should return `professors, courses`. 
                             If there are no relevant collections, simple return 'There are no relevant collections'
                             User Query: {user_query}. Here are the schemas: {all_schemas}""").content


def create_mongodb_query(user_query: str, collection_schemas: str) -> str:
    """Takes query instructions and creates a MongoDB query.

    Args:
        user_query: Instructions for creating the MongoDB query
        collection_schemas: Schemas for the collections

    Returns:
        str: MongoDB query string
    """
    gpt4o_chat = ChatOpenAI(model="gpt-4o", temperature=0)
    prompt = f"""Use your knowledge of the following collection schemas to create a pymongo query for the following user query. 
                Under no circumstances should you return anything more than the pymongo query. Ex: db.rooms.count_documents({{}}). 
                Below are some rules about the schema to help you create the query:
                             
                <schema_rules> 
                1. Under no circumstances should you query for properties of a collection that are not listed in the schema.
                1. The word 'rooms' and 'classrooms' are synonymous. Classrooms is ont a subset of rooms. 
                2. The word 'professors' and 'faculty' are synonymous.
                3. The word 'courses' and 'classes' are synonymous.
                4. The word 'departments' and 'depts' are synonymous.
                </schema_rules>

                <user_query>
                {user_query}. 
                </user_query>

                <mongo_collection_schemas> 
                {collection_schemas}
                </mongo_collection_schemas>"""
    return gpt4o_chat.invoke(prompt).content

def update_plan(goal: str, current_plan: str, completed_steps: str, new_observations: str, available_tools: str) -> str:
    """Updates an existing plan based on new observations and progress.

    Args:
        goal: The overall goal being worked towards
        current_plan: The current plan steps
        completed_steps: Steps that have been completed so far
        new_observations: New information from the environment
        available_tools: String containing available tools and their descriptions

    Returns:
        str: Updated plan from GPT-4
    """
    gpt4o_chat = ChatOpenAI(model="gpt-4o", temperature=0)
    
    prompt = f"""You are a planner for an AI agent. The agent has been working on the following goal:

Goal: {goal}

Current plan:
{current_plan}

Progress so far:
{completed_steps}

New information from environment:
{new_observations}

Available tools:
{available_tools}

Rules to Follow: 
1. Under no circumstances should you query for properties of a collection that are not listed in the schema.
2. The word 'rooms' and 'classrooms' are synonymous. Classrooms is ont a subset of rooms. 
3. The word 'professors' and 'faculty' are synonymous.
4. The word 'courses' and 'classes' are synonymous.
5. The word 'departments' and 'depts' are synonymous.
6. If there are no relevant collections, terminate the plan and tell the user that we don't have any information on that topic.

Based on this new information and the tools available, update the remaining steps of the plan. Add, remove, or modify steps as needed. Make sure to only include steps that can be accomplished using the available tools. Provide the updated plan as a numbered list."""

    return gpt4o_chat.invoke(prompt).content



def execute_pymongo(query: str) -> str:
    """Executes a PyMongo query against the specified database.

    Args:
        query: PyMongo query to execute as a string
    
    Returns:
        str: Results from executing the PyMongo query
    """
    # Connect to MongoDB
    client = MongoClient("mongodb+srv://cd-staging-developers:RaiisIbw5N3xeO3m@cluster0-j5tkd.mongodb.net/coursedog")
    db = client.abraham_baldwin

    try:
        # Execute the query
        results = eval(query)
        
        # Handle cursor objects from find() operations
        if hasattr(results, 'collection'):
            results = list(results)
            
        return dumps(results)

    except Exception as e:
        return str(e)
    finally:
        client.close()


def execute_mongodb_shell_syntax(shell_syntax: str) -> str:
    """Executes MongoDB shell syntax against the specified database.

    Args:
        shell_syntax: MongoDB shell syntax to execute as a string
    
    Returns:
        str: Results from executing the MongoDB shell command
    """
    print("Executing MongoDB shell syntax: ", shell_syntax)

    # Connect to MongoDB
    client = MongoClient("mongodb+srv://cd-staging-developers:RaiisIbw5N3xeO3m@cluster0-j5tkd.mongodb.net/coursedog")
    db = client.abraham_baldwin

    try:
        # Parse the shell syntax and remove 'db.' prefix if present
        parts = shell_syntax.split('.')
        if parts[0] == 'db':
            parts = parts[1:]  # Remove 'db' prefix
        
        collection_name = parts[0]
        operation = parts[1]
        
        collection = db[collection_name]
        
        # Handle different operations
        if operation == 'count()':
            results = collection.count_documents({})
        elif operation.startswith('find'):
            # Handle existing find logic
            results = eval(f"collection.{operation}")
            if hasattr(results, 'collection'):
                results = list(results)
        else:
            results = eval(f"collection.{operation}")
            
        return dumps(results)

    except Exception as e:
        return str(e)
    finally:
        client.close()


def mongodb_schemas_for_collections(collections: list[str]) -> list[str]:
    """Takes the list of collections and returns the schemas for each collection.

    Args:
        collections: list of collections
    """
    
    return [schema for schema in all_schemas if schema["collection"] in collections]

def multiply(a: int, b: int) -> int:
    """Multiplies a and b.

    Args:
        a: first int
        b: second int
    """
    return a * b





def divide(a: int, b: int) -> float:
    """Divide a and b.

    Args:
        a: first int
        b: second int
    """
    return a / b

tools = [update_plan, add, multiply, divide, identify_relevant_mongodb_collections, mongodb_schemas_for_collections, create_mongodb_query, execute_pymongo]

# Define LLM with bound tools
llm = ChatOpenAI(model="gpt-4o")
llm_with_tools = llm.bind_tools(tools)

# System message
sys_msg = SystemMessage(content="""
You are a helpful assistant tasked with helping users query the university's database. 
Before each step that you take, call the 'update_plan' tool to create a plan for how to accomplish the user's query.
Execute the next step of your plan. After you execute a step, call the 'update_plan' tool again to update your plan based on the results of the step.

Below are some rules on how to treat the results from tools. 
Tool Usage Rules: 
If you get a response back from 'create_mongodb_query' tool, be sure to use it as is and never modify it.
                        
General Rules and Guidance to Follow: 
1. Under no circumstances should you query for properties of a collection that are not listed in the schema.
2. The word 'rooms' and 'classrooms' are synonymous. Classrooms is ont a subset of rooms. 
3. The word 'professors' and 'faculty' are synonymous.
4. The word 'courses' and 'classes' are synonymous.
5. The word 'departments' and 'depts' are synonymous.
""")

# Node
def assistant(state: MessagesState):
   return {"messages": [llm_with_tools.invoke([sys_msg] + state["messages"])]}

# Build graph
builder = StateGraph(MessagesState)
builder.add_node("assistant", assistant)
builder.add_node("tools", ToolNode(tools))
builder.add_edge(START, "assistant")
builder.add_conditional_edges(
    "assistant",
    # If the latest message (result) from assistant is a tool call -> tools_condition routes to tools
    # If the latest message (result) from assistant is a not a tool call -> tools_condition routes to END
    tools_condition,
)
builder.add_edge("tools", "assistant")

# Compile graph
graph = builder.compile()
