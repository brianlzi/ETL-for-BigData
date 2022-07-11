from confluent_kafka.admin import AdminClient, NewTopic


admin_client = AdminClient({
    "bootstrap.servers": "localhost:29092"
})

topic_list = []
topic_list.append(NewTopic("topic_json", 1, 1))
admin_client.create_topics(topic_list)