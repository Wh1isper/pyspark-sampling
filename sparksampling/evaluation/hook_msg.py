class HookMsg(object):
    def __init__(self, hook, msg):
        self.hook_name = hook.__name__ if isinstance(hook, type) else hook.__class__.__name__
        self.msg = msg

    def generate_proto_msg(self, period):
        proto_pair_msg = []
        for k, v in self.msg.items():
            proto_pair_msg.append({"key": str(k), "value": str(v)})

        return {
            "hook_name": self.hook_name,
            "period": period,
            "meta": proto_pair_msg,
        }
