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
    
class ReferenceDataframe(BaseModel):
    df_name: Optional[str]
    column_name: Optional[str]

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
    
    __context__: Context
    __ref_context__: Context
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
    
    def create_context(self, df_group: dict[str, pd.DataFrame], rules: HandleEventRules):
        self.__context__ = Context(df=df_group[rules.df_name], column_name=rules.column_name).initialize()
        if rules.reference != None: self.__ref_context__ = Context(df=df_group[rules.reference.df_name], column_name=rules.reference.column_name)
        
    def set_index(self, index):
        self.__context__.index = index
        if self.__ref_context__ != None: self.__ref_context__.index = index
        
    def check_event(self, **kwargs) -> bool:
        if self.__event_function__(self.__context__, **kwargs) and (self.both and self.__event_function__(self.__ref_context__, **kwargs)): return True
        else: return False
    
    def run_action(self, **kwargs):
        self.__context__.df = self.__action_function__(**kwargs)
        

def handle_event(df_group: dict[str, pd.DataFrame], event: Event, action: Action, rules: HandleEventRules) -> dict[str, pd.DataFrame]:
    event_handler = EventHandler(event_type=event.type, action_type=action.type)
    event_handler.create_context(df_group==df_group, rules=rules)
    
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