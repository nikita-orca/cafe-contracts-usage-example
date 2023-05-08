import faust

app = faust.App("cafe", broker="kafka:9092", value_serializer="json")

ORDERS_INPUT_TOPIC = app.topic("internal_unclear_topic_name")
INSTRUCTIONS_OUTPUT_TOPIC = app.topic("another_hebrish_topic_name")


@app.agent(ORDERS_INPUT_TOPIC)
async def process_order(order_stream):
    async for order_dict in order_stream:
        info_for_barista = "Take money first and prioritize premium clients instead. "
        use_expensive_seeds = False
        time_limit_in_minutes = 10

        if order_dict["premium_service"]:
            info_for_barista = "Be friendly and put a name on a coffee cup. "
            time_limit_in_minutes = 2
            use_expensive_seeds = True

        info_for_barista += f"Prepare {order_dict['coffee_type']}"

        await INSTRUCTIONS_OUTPUT_TOPIC.send(
            value=dict(
                customer_id=order_dict["customer_id"],
                info_for_barista=info_for_barista,
                time_limit_in_minutes=time_limit_in_minutes,
                use_expensive_seeds=use_expensive_seeds,
            )
        )


if __name__ == "__main__":
    app.main()
