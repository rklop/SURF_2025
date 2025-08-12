from constants import DIALECT
from environment import Environment

def verify_sql_equivalence(sql1, sql2, schema, ROW_NUM=2, constraints=None, **kwargs):

    with Environment(**kwargs) as env:
        for k, v in schema.items():
            env.create_database(attributes=v, bound_size=ROW_NUM, name=k)
        env.add_constraints(constraints)
        env.save_checkpoints()
        if env._script_writer is not None:
            env._script_writer.save_checkpoints()
        
        
        result = env.analyze(sql1, sql2)
        counterexample = env.counterexample if env.show_counterexample else None
        time_cost = None
        if env.traversing_time is not None:
            if env.solving_time is not None:
                time_cost = env.traversing_time + env.solving_time
        return {
            'equivalent': bool(result is True),
            'counterexample': counterexample,
            'time_cost': time_cost
        }
