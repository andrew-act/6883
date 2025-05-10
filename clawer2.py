import requests
import pandas as pd
import json
from pandas import json_normalize

# #api_key =  "2b465d4d9e6e69ae8071cfb2e8b88bae"
url = "https://gateway.thegraph.com/api/2b465d4d9e6e69ae8071cfb2e8b88bae/subgraphs/id/A3Np3RQbaBA6oKJgiwDJeo5T3zrYfGHPWFYayMwtNDum"
#
# ##GraphQL introspection Query for a certain type of event Type Field
# # query = """
# # {
# #   __type(name: "Burn") {
# #     name
# #     fields {
# #       name
# #       type {
# #         name
# #         kind
# #       }
# #     }
# #   }
# # }

# define the query that looking for transation datas
queries = """
{
  swaps(first: 200, orderBy: timestamp, orderDirection: desc) {
    id
    transaction {
      id
      timestamp
    }
    amount0In
    amount0Out
    amount1In
    amount1Out
    amountUSD
  }
  mints(first: 200, orderBy: timestamp, orderDirection: desc) {
    id
    transaction {
      id
    }
    timestamp
    pair {
      token0 {
        symbol
      }
      token1 {
        symbol
      }
    }
    to
    liquidity
    sender
    amount0
    amount1
    logIndex
    amountUSD
  }
  burns(first: 200, orderBy: timestamp, orderDirection: desc) {
    id
    transaction {
      id
    }
    timestamp
    pair {
      token0 {
        symbol
      }
      token1 {
        symbol
      }
    }
    liquidity
    sender
    amount0
    amount1
    to
    logIndex
    amountUSD
    needsComplete

  }
  uniswapDayDatas(first: 100, orderBy: date, orderDirection: desc) {
    id
    date
    dailyVolumeETH
    dailyVolumeUSD
    dailyVolumeUntracked
    totalVolumeETH
    totalLiquidityETH
    totalVolumeUSD
    totalLiquidityUSD
    txCount
  }
  pairDayDatas(first: 100, orderBy: date, orderDirection: desc) {
    id
    date
    pairAddress
    token0 {
      symbol
    }
    token1 {
      symbol
    }
    reserve0
    reserve1
    totalSupply
    reserveUSD
    dailyVolumeToken0
    dailyVolumeToken1
    dailyVolumeUSD
    dailyTxns
  }
  tokenDayDatas(first: 100, orderBy: date, orderDirection: desc) {
    id
    date
    token {
      id
      symbol
    }
    dailyVolumeToken
    dailyVolumeETH
    dailyVolumeUSD
    dailyTxns
    totalLiquidityToken
    totalLiquidityETH
    totalLiquidityUSD
    priceUSD
  }
}
"""
# using post to fetch data
response = requests.post(url, json={'query': queries})
data = response.json()

# extract date from json
burns_data = data['data']['burns']
swaps_data = data['data']['swaps']
mints_data = data['data']['mints']
uniswapDayDatas_data = data['data']['uniswapDayDatas']
pairDayDatas_data = data['data']['pairDayDatas']
tokenDayDatas_data = data['data']['tokenDayDatas']

# deal with data, and save as xlsx file
def burns(burns_data):
# Expand the nested pair structure
    for item in burns_data:
        if 'pair' in item:
            item['token0_symbol'] = item['pair']['token0']['symbol']
            item['token1_symbol'] = item['pair']['token1']['symbol']
            del item['pair']  # del pai

        if 'transaction' in item:
            item['transaction_id'] = item['transaction']['id']
            del item['transaction']  # del transaction

    # convert to DataFrame
    df = pd.DataFrame(burns_data)
    # save as excel file
    output_file = 'burns_data.xlsx'
    df.to_excel(output_file, index=False)
    print(f"data save {output_file}")

# follow the same structure as above, deal with rest 5 data set, save them as excel file
def swaps(swaps_data):
    for item in swaps_data:
        if 'transaction' in item:
            item['transaction_id'] = item['transaction']['id']
            item['transaction_timsstamp'] = item['transaction']['timestamp']
            del item['transaction']
    df = pd.DataFrame(swaps_data)
    output_file = 'swaps_data.xlsx'
    df.to_excel(output_file, index=False)
    print(f"data save {output_file}")

def mints(mints_data):
    for item in mints_data:
        if 'pair' in item:
            item['token0_symbol'] = item['pair']['token0']['symbol']
            item['token1_symbol'] = item['pair']['token1']['symbol']
            del item['pair']
        if 'transaction' in item:
            item['transaction_id'] = item['transaction']['id']
            del item['transaction']
    df = pd.DataFrame(mints_data)
    output_file = 'mints_data.xlsx'
    df.to_excel(output_file, index=False)
    print(f"data save {output_file}")


def uniswapDayDatas(uniswapDayDatas_data):
    df = pd.DataFrame(uniswapDayDatas_data)
    output_file = 'uniswapDayDatas_data.xlsx'
    df.to_excel(output_file, index=False)
    print(f"data save {output_file}")

def pairDayDatas(pairDayDatas_data):
    for item in pairDayDatas_data:
        if 'token0' in item:
            item['token0'] = item['token0']['symbol']
            del item['token0']
        if 'token1' in item:
            item['token1'] = item['token1']['symbol']
            del item['token1']
    df = pd.DataFrame(pairDayDatas_data)
    output_file = 'pairDayDatas_data.xlsx'
    df.to_excel(output_file, index=False)
    print(f"data save {output_file}")

def tokenDayDatas(tokenDayDatas_data):
    for item in tokenDayDatas_data:
        if 'token' in item:
            item['token_id'] = item['token']['id']
            item['token_symbol'] = item['token']['symbol']
            del item['token']  # del token

    df = pd.DataFrame(tokenDayDatas_data)
    # save as excel file
    output_file = 'tokenDayDatas.xlsx'
    df.to_excel(output_file, index=False)
    print(f"data save {output_file}")



# call these function to save data
swaps(swaps_data)
burns(burns_data)
mints(mints_data)
uniswapDayDatas(uniswapDayDatas_data)
pairDayDatas(pairDayDatas_data)
tokenDayDatas(tokenDayDatas_data)
