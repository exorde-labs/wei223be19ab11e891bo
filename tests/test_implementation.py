from wei223be19ab11e891bo import query
from exorde_data.models import Item
import pytest


@pytest.mark.asyncio
async def test_query():
    urls = []
    _item_count = 0    
    parameters = {
        "max_oldness_seconds":3000,
        "max_consecutive_old_posts":3,
        "maximum_items_to_collect": 5,        
        "min_post_length": 10,        
        'keywords': ["比特币", "以太坊", "ETH", "BTC", "USDT"],
        'url': "https://weibo.com/login.php"
    }

    try:
        async for item in query(parameters):
            assert isinstance(item, Item)
            print("\n")
            print("Date = ",item['created_at'])
            print("Content = ", item['content'])
            print("author = ",item['author'])
            print("url = ",item['url'])
            urls.append(item.url)
            _item_count += 1
    except ValueError as e:
        print(f"Error: {str(e)}")
