"""Create a generator that yields fake user data."""
from typing import Any

from faker import Faker


fake = Faker(locale='en_GB')

def get_users(
    start_id: int,
    number_rows: int=10,
) -> dict[str, Any]:
    for i in range(start_id, start_id+number_rows):
        yield {
            'id': i,
            'user_id': fake.random_int(min=0, max=1_000),
            'name': fake.name(),
            'alias': [fake.name() for _ in range(fake.random_int(min=0, max=3))],
            'phone_number': fake.phone_number(),
            'email': fake.email(),
            'address': {
                'street': fake.street_address(),
                'city': fake.city(),
                'postal_code': fake.postcode()
            },
            'non_informative': None,
            'status': fake.random_element(elements=('active', 'inactive')),\
        }

  
def get_events(
    user_id: int,
    number_rows: int=10,
) -> dict[str, Any]:
    for i in range(number_rows):
        yield {
            'user_id': user_id,
            'event_id': fake.random_int(min=0, max=10_000),
            'event_name': fake.random_element(elements=('breach', 'alert', 'incident')),
            'event_date': fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d'),
            'event_time': fake.time(pattern='%H:%M:%S'),
            'event_location': fake.local_latlng(country_code='GB', coords_only=True),
            'event_description': fake.text(),
        }


if __name__ == "__main__":
    from datetime import datetime
    from pathlib import Path

    import pandas as pd


    data_dir = Path(__file__).parent.parent / 'data'
    data_dir.mkdir(exist_ok=True)

    for i in range(3):
        timestamp = fake.date_time_between(start_date='-3M', end_date='now').strftime('%Y%m%d_%H%M%S')

        users = pd.DataFrame(get_users(start_id=i*10))
        users.to_json(
            data_dir/ f'users_{timestamp}.jsonl',
            orient='records',
            lines=True,
        )

        events = []
        for user_id in users.loc[:, 'user_id']:
            events.append(pd.DataFrame(get_events(
                user_id=user_id,
                number_rows=fake.random_int(min=0, max=10),
            )))
        
        pd.concat(events).to_json(
            data_dir/ f'events_{timestamp}.jsonl',
            orient='records',
            lines=True,
        )