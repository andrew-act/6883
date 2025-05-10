from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

url = "https://gateway.thegraph.com/api/2b465d4d9e6e69ae8071cfb2e8b88bae/subgraphs/id/A3Np3RQbaBA6oKJgiwDJeo5T3zrYfGHPWFYayMwtNDum"

transport = RequestsHTTPTransport(
    url=url,
    verify=True,
    retries=3,
)
client = Client(transport=transport, fetch_schema_from_transport=True)

# introspection 查询所有类型（相当于事件实体）
query = gql("""
{
  __schema {
    types {
      name
    }
  }
}
""")

result = client.execute(query)

# 打印所有类型名称
for type_ in result['__schema']['types']:
    print(type_['name'])


