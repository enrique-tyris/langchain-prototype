import os
import sys
from dotenv import load_dotenv
from langchain.prompts.prompt import PromptTemplate
from langchain_google_vertexai import ChatVertexAI
import vertexai

# Configurar path y variables de entorno al inicio
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)  # Agregar la raíz del proyecto al path
env_path = os.path.join(project_root, '.env')
load_dotenv(env_path)

# Initialize Vertex AI
vertexai.init(
    project=os.getenv("GOOGLE_CLOUD_PROJECT"),
    location="europe-west1"
)

from tools.third_parties.linkedin import scrape_linkedin_profile
from experiments.linkedin_lookup_chain import lookup as linkedin_lookup_agent


def ice_break_with(name: str) -> str:
    linkedin_username = linkedin_lookup_agent(name=name)
    linkedin_data = scrape_linkedin_profile(linkedin_profile_url=linkedin_username)

    summary_template = """
    given the Linkedin information {information} about a person I want you to create:
    1. A short summary
    2. two interesting facts about them
    """
    summary_prompt_template = PromptTemplate(
        input_variables=["information"], template=summary_template
    )

    llm = ChatVertexAI(
        model=os.environ["CHAT_MODEL"],
        temperature=0
    )

    chain = summary_prompt_template | llm

    res = chain.invoke(input={"information": linkedin_data})

    # Parsear la respuesta para mostrar solo el contenido
    if hasattr(res, 'content'):
        print(res.content)
        print('\n')
    else:
        print(res)


if __name__ == "__main__":
    print("Ice Breaker Enter")
    ice_break_with(name="Eden Marco")
