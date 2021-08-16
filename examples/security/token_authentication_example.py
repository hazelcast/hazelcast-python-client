import hazelcast
from hazelcast.security import BasicTokenProvider

# Use the following configuration in the member-side.
#
# <security enabled="true">
#     <client-permissions>
#         <map-permission name="auth-map" principal="*">
#             <actions>
#                 <action>create</action>
#                 <action>destroy</action>
#                 <action>put</action>
#                 <action>read</action>
#             </actions>
#         </map-permission>
#     </client-permissions>
#     <member-authentication realm="tokenRealm"/>
#     <realms>
#         <realm name="tokenRealm">
#              <identity>
#                 <token>s3crEt</token>
#             </identity>
#         </realm>
#     </realms>
# </security

# Start a new Hazelcast client with the given token provider.
token_provider = BasicTokenProvider("s3crEt")
client = hazelcast.HazelcastClient(token_provider=token_provider)

hz_map = client.get_map("auth-map").blocking()
hz_map.put("key", "value")

print(hz_map.get("key"))

client.shutdown()
