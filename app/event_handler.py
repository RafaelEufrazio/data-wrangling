import pandas as pd
from enum import Enum
from typing import Type, Callable, Self, Optional
from pydantic import BaseModel

class EventType(Enum):
    ABRUPT = 1
    STAGNANT = 2

class Event(BaseModel):
    type: Type[EventType]
    args: dict[str, any]
    
class ActionType(Enum):
    SUBSTITUTE = 1

class Action(BaseModel):
    type: Type[ActionType]
    args: dict[str, any]
    
class ReferenceComparisonType(Enum):
    NO_COMPARISON = 1
    ONLY = 2
    BOTH = 3

class ReferenceDataframe(BaseModel):
    df_name: str
    column_name: str
    comparison: Type[ReferenceComparisonType]

class HandleEventRules(BaseModel):
    df_name: str
    column_name: str
    reference: Optional[ReferenceDataframe]

class Context(BaseModel):
    df: pd.DataFrame
    column_name : str
    
    index: any
    previous_index: any
    previous_value: any
    window_count: int
    window_start: any
    
    def initialize(self, starting_index: int = 0) -> Self:
        self.index = starting_index
        self.previous_index = self.df.index[self.index]
        self.previous_value = self.df.iloc[self.index][self.column_name]
        self.window_count = 0
        self.window_start = None
        return self
    
    def __iter__(self):
        yield self.df
        yield self.column_name
        yield self.index
        yield self.previous_index
        yield self.previous_value
        yield self.window_count
        yield self.window_start

class EventHandler(BaseModel):
    event_type: Type[EventType]
    __event_function__: Callable[..., bool]
    
    action_type: Type[ActionType]
    __action_function__: Callable[..., pd.DataFrame]
    
    rules: HandleEventRules
    __comparison_function__: Callable[[bool, bool], bool]
    
    __context__: Context
    __ref_context__: Context
    
    def __init__(self, both: bool = False, **kwargs):
        super.__init__(**kwargs)
        self.both = both
        self.set_event_function()
        self.set_action_function()
        self.set_comparison_function()
        
    def set_event_function(self):
        match self.event_type:
            case EventType.ABRUPT:
                self.__event_function__ = abrupt
            case EventType.STAGNANT:
                self.__event_function__ = stagnant

    def set_action_function(self):
        match self.action_type:
            case ActionType.SUBSTITUTE:
                self.__action_function__ = substitute
                
    def set_comparison_function(self):
        if self.rules.reference == None: return
        match self.rules.reference.comparison:
            case ReferenceComparisonType.ONLY:
                self.__comparison_function__ = lambda a, b: a and not b
            case ReferenceComparisonType.BOTH:
                self.__comparison_function__ = lambda a, b: a and b
            # The no comparison case houldn't be called but is left here just in case
            case ReferenceComparisonType.NO_COMPARISON:
                self.__comparison_function__ = lambda a, _: a    
    
    def create_context(self, df_group: dict[str, pd.DataFrame]):
        self.__context__ = Context(df=df_group[self.rules.df_name], column_name=self.rules.column_name).initialize()
        if self.rules.reference != None: self.__ref_context__ = Context(df=df_group[self.rules.reference.df_name], column_name=self.rules.reference.column_name)
        
    def set_index(self, index):
        self.__context__.index = index
        if self.__ref_context__ != None: self.__ref_context__.index = index
        
    def check_event(self, **kwargs) -> bool:
        check = self.__event_function__(self.__context__, **kwargs)
        if self.rules.reference == None or self.rules.reference.comparison == ReferenceComparisonType.NO_COMPARISON: return check
        return self.__comparison_function__(check, self.__event_function__(self.__ref_context__, **kwargs))
    
    def run_action(self, **kwargs):
        self.__context__.df = self.__action_function__(**kwargs)
        

def handle_event(df_group: dict[str, pd.DataFrame], event: Event, action: Action, rules: HandleEventRules) -> dict[str, pd.DataFrame]:
    event_handler = EventHandler(event_type=event.type, action_type=action.type, rules=rules)
    event_handler.create_context(df_group==df_group)
    
    for index, row in event_handler.__context__.df.index():
        event_handler.set_index(index)
        if event_handler.check_event(event.args):
            event_handler.run_action(action.args)

 
def substitute():
    return

def abrupt() -> bool:
    return

def stagnant() -> bool:
    return