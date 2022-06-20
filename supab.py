from supabase_client import Client
import asyncio
import json

supabase = Client( 
	api_url="http://fscdhnxariefesouconj.supabase.co",
	api_key="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYW5vbiIsImlhdCI6MTYyODMxNjA1NCwiZXhwIjoxOTQzODkyMDU0fQ.Y52JP3qH9Jt-pAe7Q_PwGCyBeL8epZ5C8s1rFfTlcjM",
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYW5vbiIsImlhdCI6MTYyODMxNjA1NCwiZXhwIjoxOTQzODkyMDU0fQ.Y52JP3qH9Jt-pAe7Q_PwGCyBeL8epZ5C8s1rFfTlcjM"
    }
)



async def main():
    data = []
    error, data = await (
        supabase.table("posts")
        .select("*")
        .query()
    )

    # with open("dev.json", "w") as f:
    #     json.dump(data, f)

    # print(len(data))


asyncio.run(main())