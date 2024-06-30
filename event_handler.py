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

class EventRules(BaseModel):
    column_name: str
    substitute_df_name: Optional[str]
    substitute_column_name: Optional[str]
    
class HandleEventRules(BaseModel):
    dfs: dict[str, EventRules]
    both: bool

class Context(BaseModel):
    df: pd.DataFrame
    column_name : str
    
    index: any
    previous_index: any
    previous_value: any
    window_count: any
    window_start: any
    
    def initialize(self, starting_index: int = 0) -> Self:
        self.index = starting_index
        self.previous_index = self.df.index[self.index]
        self.previous_value = self.df.iloc[self.index][self.column_name]
        self.window_count = 0
        self.window_start = None
        return self

class EventHandler(BaseModel):
    event_type: Type[EventType]
    __event_function__: Callable[..., bool]
    
    action_type: Type[ActionType]
    __action_function__: Callable[..., pd.DataFrame]
    
    __context__: Context
    __sub_context__: Context
    both: bool
    
    def __init__(self, both: bool = False, **kwargs):
        super.__init__(**kwargs)
        self.both = both
        self.set_event_function()
        self.set_action_function()
        
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
    
    def create_context(self, df: pd.DataFrame, column_name: str, substitute: bool = False):
        context = Context(df=df, column_name=column_name).initialize()
        if not substitute: self.__context__ = context
        else: self.__sub_context__ = context
        
    def set_index(self, index):
        self.__context__.index = index
        if self.__sub_context__ != None: self.__sub_context__.index = index
        
    def check_event(self, **kwargs) -> bool:
        if self.__event_function__(self.__context__, **kwargs) and (self.both and self.__event_function__(self.__sub_context__, **kwargs)): return True
        else: return False
    
    def run_action(self, **kwargs):
        self.__context__.df = self.__action_function__(**kwargs)
        

def handle_event(df_group: dict[str, pd.DataFrame], event: Event, action: Action, rules: HandleEventRules) -> dict[str, pd.DataFrame]:
    event_handler = EventHandler(event_type=event.type, action_type=action.type)
    for df_name, event_rules in rules.dfs.items():
        event_handler.create_context(df=df_group[df_name], column_name=event_rules.column_name)
        if rules.both: event_handler.create_context(df=df_group[event_rules.substitute_df_name], column_name=event_rules.substitute_column_name)
        
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