from src.repository import fileRepository
from src.repository import marketTypeRepository
from src.repository import bdiRepository
from src.repository import priceCorrectionRepository
from src.repository import companyRepository
from src.repository import paperRepository
from src.repository import pregaoRepository
from src.service import fileService

from sqlalchemy import  MetaData, Table, func
from sqlalchemy.orm import sessionmaker
from datetime import  datetime
import pytz
import os
import pendulum
from airflow.decorators import dag, task

@dag(
    schedule='0 */6 * * * ',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["pievi"],
)

def updateLastDay():
    @task
    def getFile():
        fuso_horario_brasil = pytz.timezone('America/Sao_Paulo')
        today = datetime.now(fuso_horario_brasil).date().strftime("%d%m%Y")
        print(today)
        
        name_file = f'COTAHIST_D{today}.TXT'
        file_path = os.path.join('/opt/airflow/dags/src/data', name_file)
        return file_path
    
    @task
    def updateStage(file_path):
        engine = fileRepository.create_enginer()

        metadata = MetaData()
        btres = Table('btres', metadata, autoload=True, autoload_with=engine)
        
        Session = sessionmaker(bind=engine)
        session = Session()

        last_date = session.query(func.max(btres.c.data_pregao)).scalar()
        
        fuso_horario_brasil = pytz.timezone('America/Sao_Paulo')
        today = datetime.now(fuso_horario_brasil).date()

        if last_date and last_date < today:
            formattedFile = fileService.fommaterb3(file_path)
            formattedFile.to_sql('btres', engine, if_exists='append', index=False)

        session.close()

    @task
    def updateStarSchema():
        conn = fileRepository.connectBdd()

        marketTypeRepository.create_table_market_type(conn)
        marketTypeRepository.insert_table_market_type(conn)

        bdiRepository.create_table_bdi(conn)
        bdiRepository.insert_table_bdi(conn)
    
        priceCorrectionRepository.create_table_price_correction(conn)
        priceCorrectionRepository.insert_table_price_correction(conn)

        companyRepository.create_table_company(conn)
        companyRepository.insert_table_company(conn)

        paperRepository.create_table_paper(conn)
        paperRepository.insert_table_paper(conn)

        pregaoRepository.create_table_pregao(conn)
        pregaoRepository.insert_table_pregao(conn)

        pregaoRepository.create_relation(conn)


    file_path = getFile()
    stage = updateStage(file_path)
    updateStarSchemas = updateStarSchema()
    file_path >> stage >> updateStarSchemas

updateLastDay()