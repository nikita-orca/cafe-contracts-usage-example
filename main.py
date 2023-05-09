import faust
from orca.contracts.coffee_ordered import CoffeeOrdered
from orca.contracts.contract_utils import ContractDetails, ContractIdentifier
from orca.contracts.order_instructions_recieved import \
    OrderInstructionsRecieved

app = faust.App("cafe", broker="kafka:9092", value_serializer="json")

ORDER_INPUT_CONTRACT_ID = ContractIdentifier(
    domain="cafe",
    contract_name="coffee_ordered",
    contract_version=1,
)
ORDER_INSTRUCTIONS_OUTPUT_CONTRACT_ID = ContractIdentifier(
    domain="cafe",
    contract_name="order_instructions_recieved",
    contract_version=1,
)

ORDERS_INPUT_TOPIC = app.topic(
    ContractDetails(ORDER_INPUT_CONTRACT_ID).get_channel_name(include_env_prefix=True)
)
INSTRUCTIONS_OUTPUT_TOPIC = app.topic(
    ContractDetails(ORDER_INSTRUCTIONS_OUTPUT_CONTRACT_ID).get_channel_name(
        include_env_prefix=True
    )
)


@app.agent(ORDERS_INPUT_TOPIC)
async def process_order(order_stream):
    async for order_dict in order_stream:
        order = CoffeeOrdered.parse_obj(order_dict)
        info_for_barista = "Take money first and prioritize premium clients instead. "
        use_expensive_seeds = False
        time_limit_in_minutes = 10

        if order.premium_service:
            info_for_barista = "Be friendly and put a name on a coffee cup. "
            time_limit_in_minutes = 2
            use_expensive_seeds = True

        info_for_barista += f"Prepare {order.coffee_type}"

        await INSTRUCTIONS_OUTPUT_TOPIC.send(
            value=OrderInstructionsRecieved(
                customer_id=order.customer_id,
                info_for_barista=info_for_barista,
                time_limit_in_minutes=time_limit_in_minutes,
                use_expensive_seeds=use_expensive_seeds,
            ).dict()
        )


if __name__ == "__main__":
    app.main()
