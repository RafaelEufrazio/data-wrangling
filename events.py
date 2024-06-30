from pandas import pd
from event_handler import Context

def abrupt(context: Context, threshold: int) -> bool:
    df, column_name, index, previous_index, previous_value = context
    is_abrupt = False
    
    # First we need to check if the current and previous indexes both exist in the dataframe
    if not index in df.index: return False
    if not previous_index in df.index: return False

    if abs(df.loc[index][column_name] - previous_value) >= threshold:
        is_abrupt = True

    # TODO: Suellen code added another condition after this, which would check if the following
    # line was also abrupt, which would mean that this would be the faulty line.
    # This would require more than one event checks to be ran before an action could happen.
        
    context.previous_index = index
    context.previous_value = df.loc[index][column_name]
    
    return is_abrupt