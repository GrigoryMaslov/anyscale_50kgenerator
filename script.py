from openai import AsyncOpenAI
import pandas as pd
import time
import asyncio

file_name = '50kxlsx.xlsx'

# Fix for parsing output failure
df = pd.read_excel(file_name)
df = pd.read_excel(file_name)
df['length'] = df.bio.str.len()

df = df[df.length < 5000]
del df["length"]
df.reset_index(drop=True, inplace=True)


# Initializing a client
client = AsyncOpenAI(
    base_url="https://api.endpoints.anyscale.com/v1",
    api_key='esecret_l1u7wsuj3ai8p1hqx4p3651mk7')


async def get_compliment(bio, username):
    result = await client.chat.completions.create(
        model='mistralai/Mistral-7B-Instruct-v0.1',
        temperature=0.6,
        messages=[
            {'role': 'user', 'content': f'''Instagram bio: Illustrator and graphic designer 🇮🇹 ✏️Info/contact carmen.ursino@gmail.com carmenursino.dribbble.com/.
            Read the Instagram bio provided above and please ensure to follow ALL rules listed please don't skip any of them.
            Rule 1: Write a casual, not too enthusiastic compliment this Instagram page based on the information provided. Be specific and say something that can only be said to this account, it has to be unique to stand out.
            Rule 2: Don't be cringe and write in the voice of some of the best cold email marketing experts in the world.
            Rule 3: Always refer to the account as you or your.
            Rule 4: Please try to keep it short.
            Rule 5: Write three sentences.
            Rule 6: Start the sentence with 'I recently came across your Instagram page', end with 'Keep up the great work!'.
            Rule 7: Again, don't be cringe, nor overly enthusiastic.
            Rule 8: do not be overly nice.
            Compliment:
            '''},
            {'role': 'assistant', 'content': "I recently came across your Instagram page and was blown away by your illustrative works. Your portfolio showcases a truly unique style that stands out among the crowd. I admire the way you seamlessly combine detail and simplicity to create such stunning designs. Keep up the great work!"},
            {'role': 'user', 'content': f'''Instagram bio: Willkommen auf unserem offiziellen Account von chemoLine! Chemoline ist die Anlaufstelle für Chemie, Physik, Biologie und Mathematik. www.chemoline.de.
            Compliment:
            '''},
            {'role': 'assistant', 'content': "I recently came across your Instagram page and I was really impressed by the diverse range of subjects you cover. Your focus on chemistry, physics, biology, and mathematics is great and I appreciate the effort to educate and engage your audience. Keep up the great work!"},
            {'role': 'user', 'content': f'''Instagram bio: {bio}.
            Compliment:
            '''}
        ],
        timeout=15
    )

    return (username, result.choices[0].message.content)


async def get_compliments(batch, leftover):
    tasks = [get_compliment(df.loc[batch*30+i]['bio'], df.loc[batch*30+i]['username'])
             for i in range(leftover)]
    responses = await asyncio.gather(*tasks)
    return responses

total_batches = len(df)//30
last_ones = len(df) % 30
print("Total batches:", total_batches)
start = time.time()

for batch in range(total_batches+1):
    print("Logger: it's batch no.", batch)

    while True:
        try:
            if batch == total_batches:
                print('Final stage:')
                print('Batch num:', batch)
                responses = asyncio.run(get_compliments(batch, last_ones))
                for response in responses:
                    condition = (df['username'] == response[0])
                    df.loc[condition, 'compliment'] = response[1].strip(' "')
            else:
                responses = asyncio.run(get_compliments(batch, 30))
                for response in responses:
                    condition = (df['username'] == response[0])
                    df.loc[condition, 'compliment'] = response[1].strip(' "')

                if batch % 10 == 0:
                    end = time.time()
                    df.to_csv('result.csv')
                    print(f'time consumed per 300 outputs: {end - start}')
                    start = time.time()
            break

        except Exception as error:
            print("An exception occurred:", type(error).__name__, "–", error)

            # If exception is a Timeout or API connection failure,
            # re-initializing the client helps!

            # Wait for server to cool down
            time.sleep(15)

            # remove the old client
            del client

            # create new client
            client = AsyncOpenAI(
                base_url="https://api.endpoints.anyscale.com/v1",
                api_key='esecret_l1u7wsuj3ai8p1hqx4p3651mk7')

            # and try again
            continue


df.to_csv('result.csv')
