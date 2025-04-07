from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
import os

app = FastAPI()

VALID_ACCESS_KEY = os.getenv("EXPECTED_ACCESS_KEY") 

class WriteTagRequest(BaseModel):
    tagName: str
    stringValue: str
    intValue: int
    doubleValue: float
    accessKey: int  

class TagService:
    @staticmethod
    def getDbConnection():
        return psycopg2.connect(
            host=os.getenv('HOST'),
            database=os.getenv('DATABASE'),
            user=os.getenv('USER'),
            password=os.getenv('PASSWORD'),
            port=os.getenv('PORT')
        )
    @staticmethod
    def validateAccessKey(accessKey):
        if accessKey != VALID_ACCESS_KEY:
            raise HTTPException(status_code=403, detail="Access denied: Invalid access key")

    @staticmethod
    def updateTag(request: WriteTagRequest):
        TagService.validateAccessKey(request.accessKey) # Valida a accessKey antes de qualquer operacao
        try:
            conn = TagService.getDbConnection()
            cursor = conn.cursor()
            
            # Verifica se a tag existe
            selectTagQuery = "SELECT id FROM tag WHERE name = %s"
            cursor.execute(selectTagQuery, (request.tagName,))
            tagId = cursor.fetchone()
            if tagId:
                tagId = tagId[0]

                # Verifica se ja existem valores para a tag
                checkValueQuery = "SELECT * FROM tag_value WHERE tag_id = %s"
                cursor.execute(checkValueQuery, (tagId,))
                existingValue = cursor.fetchone()

                if existingValue:
                    updateQuery = """
                    UPDATE tag_value
                    SET string_value = %s, int_value = %s, double_value = %s
                    WHERE tag_id = %s
                    """
                    cursor.execute(updateQuery, (
                        request.stringValue, 
                        request.intValue, 
                        request.doubleValue, 
                        tagId
                    ))
                else:# Insere o primeiro valor caso nao exista
                    insertValueQuery = """
                    INSERT INTO tag_value (tag_id, string_value, int_value, double_value)
                    VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(insertValueQuery, (
                        tagId, 
                        request.stringValue, 
                        request.intValue, 
                        request.doubleValue
                    ))
                conn.commit()
                message = "Tag atualizada com sucesso"
            else:
                message = "Tag não encontrada. Nenhuma ação foi realizada."
            cursor.close()
            conn.close()
            return {"message": message}
        except Exception as e:
            return {"message": f"Erro ao tentar realizar escrita de tag: {e}"}


@app.put("/update-tag")
def writeTag(request: WriteTagRequest):
    return TagService.updateTag(request)
