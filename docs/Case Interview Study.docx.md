## Technical Leadership Round - Architect 

## Assignment Question 

Objective: Design a real-time streaming platform for an online shopping network (e.g., Amazon, Flipkart, Walmart Connect, or Target Roundel). This platform will process real-time data from retailer websites, capturing customer interactions (such as product views and add-to-cart actions), ad engagements (views and clicks), and other relevant events. The processed data will be utilized to generate insights via APIs, enabling marketers and retailers to optimize their advertising campaigns and enhance user engagement. 

## Candidate Expectations 

- Design Proposal: Review the detailed requirements outlined below and propose a comprehensive design. You may utilize any free online design tools (e.g., Draw.io, Lucidchart, or Word) to create and share your diagram prior to the scheduled interview. 

- Technology Choices: Clearly detail the technologies you would employ in your design. 

- Deployment Overview: Provide a visual representation of how the deployment of your proposed design would look. 

- Code design: Select a specific service from your design (e.g., an API endpoint such as GET /ad/{campaignID}/clicks for retrieving ad clicks) and implement it in the language of your choice. The focus should be on code structure, design principles, and best practices. Candidates should share their code on GitHub for review, ensuring that it is well-documented and demonstrates a clear understanding of the chosen technology stack. 

## Interview Structure 

- 40%: Candidate explains the architecture and design of the proposed solution, including technology choices and rationale. 

- 20%: Discussion on extending the design to accommodate additional requirements. 

- 20%: Code walk-through 

- 20%: In-depth review of the candidate's resume. 

## Key Requirements 

## ● Event Ingestion: 

- Identify technologies for handling high-velocity event streams from various sources while ensuring minimal latency. 

## **●** Data Processing: 

- Describe your approach for real-time event processing. 

- Explain how you would manage stateful operations, such as tracking user sessions across multiple events. 

- Specify which stream processing frameworks you would select and justify your choices. 

## **●** Data Storage: 

- Outline your design for the data storage layer to support both real-time querying and batch analytics. 

- Recommend database technologies for storing raw events and aggregated insights (e.g., Cassandra, Snowflake, DynamoDB), and discuss data retention strategies. 

## **●** Insights API Development: 

- Design APIs for accessing insights: 

   - GET /ad/{campaignID}/clicks → Returns the number of customers who clicked on the ad. 

   - GET /ad/{campaignID}/impressions → Returns the number of customers who viewed the ads. 

   - GET /ad/{campaignID}/clickToBasket → Returns the number of customers who added a product to their cart after clicking on the ad. 

- Discuss how you would structure these APIs to support both real-time and historical queries while ensuring performance and reliability. 

## **●** Scalability: 

- Explain your strategy for achieving horizontal scalability during traffic spikes, especially during peak events. 

- Discuss load balancing and partitioning strategies for event streams. 

## **●** Metrics and Monitoring: 

- Identify key metrics to monitor platform reliability and performance. 

- Specify tools for logging, alerting, and diagnostics. 

## **●** Multi-Tenancy: 

- Describe how you would implement multi-tenancy in your design to support multiple retailers on a single platform while ensuring data isolation and security. 

- Discuss strategies for managing tenant-specific configurations, data access controls, and resource allocation. 

## **●** Challenges and Trade-offs: 

- Anticipate potential challenges in designing this platform. 

- Discuss how you would balance trade-offs between real-time performance and data accuracy. 

- Cost trade-offs and continuous cost monitoring 

