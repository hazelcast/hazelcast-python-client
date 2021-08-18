import hazelcast

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
#     <member-authentication realm="passwordRealm"/>
#     <realms>
#         <realm name="passwordRealm">
#              <identity>
#                 <username-password username="member1" password="s3crEt" />
#             </identity>
#         </realm>
#     </realms>
# </security>

# Start a new Hazelcast client with the given credentials.
client = hazelcast.HazelcastClient(creds_username="member1", creds_password="s3crEt")

hz_map = client.get_map("auth-map").blocking()
hz_map.put("key", "value")

print(hz_map.get("key"))

client.shutdown()
