
from option import Option 

def get_strikes(root,exp):
    args = {"root":root,"exp":exp}
    option = Option(**args)
    strikes = option.get_list_strikes()
    return strikes  