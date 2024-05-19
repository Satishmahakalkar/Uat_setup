from aws_cdk import (
    aws_events as events,
    aws_events_targets as targets,
    aws_stepfunctions as sfn,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subscriptions,
)
from constructs import Construct


class FailureAlertSNSConstruct(Construct):

    def __init__(self, scope: Construct, id: str) -> None:
        super().__init__(scope, id)

        self.topic = sns.Topic(self, "notifyfails")
        self.topic.add_subscription(sns_subscriptions.EmailSubscription(email_address="saurabh.shirke@algonauts.in"))
        events.Rule(
            self, "notify_on_fail",
            targets=[targets.SnsTopic(self.topic)],
            event_pattern=events.EventPattern(
                source=["aws.status"],
                detail_type=["Step Functions Execution Status Change"],
                detail={ "status": ["FAILED", "TIMED_OUT"] },
            )
        )

        fail_state = sfn.Fail(self, "FailureTest")
        sfn.StateMachine(
            self, "failtest",
            definition_body=sfn.DefinitionBody.from_chainable(fail_state)
        )