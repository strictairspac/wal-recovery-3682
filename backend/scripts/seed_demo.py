import os
from sqlalchemy import select
from src.config.database import Base,engine,Session
from src.models.entities import User,LogSet,CrashCase
from src.utils.security import hash_password
RECORDS=[{'op':'begin','tx':'T1'},{'op':'write','tx':'T1','key':'sample_A','value':75},{'op':'commit','tx':'T1'},{'op':'checkpoint'},{'op':'begin','tx':'T2'},{'op':'write','tx':'T2','key':'sample_B','value':40},{'op':'begin','tx':'T3'},{'op':'write','tx':'T3','key':'sample_A','value':60},{'op':'commit','tx':'T3'},{'op':'write','tx':'T2','key':'sample_B','value':35},{'op':'commit','tx':'T2'}]
# 两笔并发事务更新不同采样点，并用区间读观察彼此，构造无法用同一先后顺序解释的冲突环。
# T1 把 A 改成 60，区间读[40,80]命中自己的 A=60 与提交态 B=50；
# T2 把 B 改成 90 后再读同一区间，两个点都不在区间内。任何串行顺序都无法同时重现两次命中，恢复时舍弃 T2。
# T4 在检查点之后只写 A=55 并按点核对（区间读仅命中自己的写入），不与 T1/T2 产生跨事务冲突，必须保留。
RANGE_RECORDS=[
 {'op':'begin','tx':'T1'},
 {'op':'begin','tx':'T2'},
 {'op':'write','tx':'T1','key':'sample_A','value':60},
 {'op':'write','tx':'T2','key':'sample_B','value':90},
 {'op':'range_read','tx':'T1','low':40,'high':80},
 {'op':'range_read','tx':'T2','low':40,'high':80},
 {'op':'commit','tx':'T1'},
 {'op':'commit','tx':'T2'},
 {'op':'checkpoint'},
 {'op':'begin','tx':'T4'},
 {'op':'write','tx':'T4','key':'sample_A','value':55},
 {'op':'range_read','tx':'T4','low':54,'high':56},
 {'op':'commit','tx':'T4'}]
def seed():
    Base.metadata.create_all(engine)
    if os.getenv('SEED_DEMO','true').lower()!='true':return
    with Session() as db:
        for role in ('admin','operator','viewer'):
            email=role+'@haili.local'
            if not db.scalar(select(User).where(User.email==email)):db.add(User(email=email,role=role,password_hash=hash_password('Recovery2026!')))
        logs=db.scalar(select(LogSet).where(LogSet.name=='海砾采样账本-晚班'))
        if logs is None:
            logs=LogSet(name='海砾采样账本-晚班',description='A初始100份、B初始50份，T1提交后检查点，T2未提交，T3已提交',initial={'sample_A':100,'sample_B':50},records=RECORDS);db.add(logs);db.flush()
        for name,point in [('断电点10-撤销T2',10),('完整提交点11',11),('初始点0',0)]:
            if not db.scalar(select(CrashCase).where(CrashCase.name==name)):db.add(CrashCase(name=name,log_set_id=logs.id,crash_after=point))
        rlogs=db.scalar(select(LogSet).where(LogSet.name=='海砾采样账本-区间读冲突'))
        if rlogs is None:
            rlogs=LogSet(name='海砾采样账本-区间读冲突',description='T1/T2并发更新不同点，区间读互相矛盾形成冲突环；T4为检查点后无冲突提交',initial={'sample_A':100,'sample_B':50},records=RANGE_RECORDS);db.add(rlogs);db.flush()
        for name,point in [('区间读-双提交冲突环8',8),('区间读-崩溃在提交前6',6),('区间读-含T4完整13',13)]:
            if not db.scalar(select(CrashCase).where(CrashCase.name==name)):db.add(CrashCase(name=name,log_set_id=rlogs.id,crash_after=point))
        db.commit()
if __name__=='__main__':seed()
