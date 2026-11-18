from pydantic import BaseModel, Field
class RecoveryInput(BaseModel):
    crash_case_id: int=Field(gt=0)
    label: str=Field(min_length=2,max_length=120)
    # 值班员录入的串行顺序（逗号或空白分隔的事务标识）与猜测的恢复余额，可留空，仅用于核对。
    expected_order: str=Field(default='',max_length=400)
    expected_balances: dict[str,int]|None=Field(default=None,max_length=30)
