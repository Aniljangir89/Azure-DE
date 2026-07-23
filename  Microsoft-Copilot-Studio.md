# Microsoft copilot studio

### idea behind MS copilot studio :

- people become more productive if they use GenAi in their day by day uses
- you can create more relavent and impact copilot than microsoft for your processes.
- this allows you to create copilot for youself, this is also know as power virtual agent.
- end to end no code platform to create customize copilots for your business.
- build once, publish every where, bost productivity.

---

##  Chatbot vs  Copilot — What's the Difference?

---

###  What is a Chatbot?

A **Chatbot** is a software application designed to **simulate a conversation** with users, typically using pre-defined rules, decision trees, or basic NLP (Natural Language Processing).

**Key Characteristics:**
- Follows **scripted flows** or rule-based logic
- Responds to specific keywords or patterns
- Limited to the **scope it was trained/programmed for**
- Mostly used for FAQs, customer support, and simple Q&A
- Does **not understand context deeply** across complex tasks
- Examples: Website support bots, IVR bots, FAQ bots

---

### 🧠 What is a Copilot?

A **Copilot** is an **AI-powered assistant** built on Large Language Models (LLMs) like GPT that works **alongside users** to help them complete complex tasks — acting as a smart, context-aware partner.

**Key Characteristics:**
- Powered by **Generative AI / LLMs** (e.g., GPT-4)
- Understands **context, intent, and nuance**
- Can **generate content**, write code, summarize, analyze, and reason
- Deeply **integrated into applications** (Microsoft 365, GitHub, Dynamics, etc.)
- Learns from **user behavior and organizational data** (with permissions)
- Examples: Microsoft 365 Copilot, GitHub Copilot, Microsoft Copilot Studio

###  Key Differences in Simple Words

> **Chatbot** = Follows instructions like a **script reader** 
> **Copilot** = Thinks, reasons, and assists like a **smart colleague** 
 
---

![alt text](images-ss/Screenshot%202026-07-08%20at%206.15.51 PM.png)

- when we creat copilot then it will create default top for use.
- there are some system topics also there which helps when user ask something that doest not matach any of these trigger phrases ,then it goes to unrecognized intent copliot does not able to match any of the topic that you make by default 
- topic means different paths where copilot may go to.

![alt text](images-ss/Screenshot%202026-07-08%20at%206.26.38 PM.png)

![alt text](images-ss/Screenshot%202026-07-08%20at%206.28.30 PM.png)

-  we dont need to type exact msg to trigger that phrashes , this use nlp to find the matching trigger and then render the output

- in simple we can say that copilot is the repo. of the topics and and each topic has of flow diagram of what happens.

- we can also create a conditional topics which is works with user response also where we define some condition . now ,the nice thing about this is ,copilot has built in its own question and answer system using adaptive cards that works realy well.


### publishing copilot 

- after building chatbot we publish that and then we use it through some chanels like if we want to use in microsoft teams, telegrams,twilio and manymore....

![alt text](images-ss/Screenshot%202026-07-08%20at%207.13.32 PM.png)

### publish to websites 


- through the same method that we did earlier we can publish our copilot in website also just in channel tab we find about the info of the publishing the copilot in website and then..


### Analytics and sessions with copilot:

- if user queries is not resolved then it will redirected to live agent is we integrate it in out copilot.
- we also can perform some analysis on that copliot like - we can fatch total conversion that we did and then we can perform analysis on that.

### disscuss about conversational design :

- we can design the workflow for out agent however we want.

- every time agent ask us questions and based on the question it will give us suitable ans.


## conditions and entity:
![alt text](images-ss/Screenshot%202026-07-09%20at%201.56.21 PM.png)

- we set come condion like user are from which country if user type that country then response will generated

- but if if user type like that i am from canada then it will not give any ans, becuase it can able to extract canada from the i am from canada that we have to extract that info from the request which is refer to entity.
- uses the entity to extract the relavent info from the users text.
- these entity are already pre build for you we dont have to write explicitly


### utilizing variable in conversation:

![alt text](images-ss/Screenshot%202026-07-09%20at%202.08.55 PM.png)

- when user select any specific option when if we can to send one more msg that  'wait you have selected {variable}' then i want to select specific variable to render that msg

![alt text](images-ss/Screenshot%202026-07-09%20at%202.11.02 PM.png)

- we use that varible in later section of the stream

![alt text](images-ss/Screenshot%202026-07-09%20at%202.18.40 PM.png)

### creating loop conversation 

- we can use concept of condtions and varibales to create a looped conversation which is pretty help full when you are talkign about conversational design where you need to ask the same question agian and again till user satisfty with your response.

- if user choose other option and want to go back then we have to perform topic management for that like go to step option 

![alt text](images-ss/Screenshot%202026-07-09%20at%202.26.46 PM.png)

![alt text](images-ss/Screenshot%202026-07-09%20at%202.28.40 PM.png)


- but there is some issue my be user can stuck into infinite loop for that what we do we just declare a variable at begining and at the end of the question box and if that that questio box lets suppose called 3 times then we will end that conversation.

