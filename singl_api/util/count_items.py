from collections import Counter

def count_items(list):
    b = dict(Counter(list))
    try:
        more_item = [key for key,value in b.items()if value > 1]
        more_item_number = [value for key, value in b.items()if value > 1]
    except IndexError as e:
        return
    else:
        return more_item, more_item_number

# list = ['e431bdb0-36bf-488f-8559-5da954562db1', 'e931bdb0-36bf-488f-8559-5da954562db1', 'e471bdb0-36bf-488f-8559-5da954562db1']
# # # #
# #
# if count_items(list)[0][0]:
#     print('ok')
# else:
#     print('noe')
