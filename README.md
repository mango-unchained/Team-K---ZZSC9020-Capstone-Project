# Welcome to ZZSC9020 GitHub repository for group K

This GitHub repository serves as the primary source control tool for group K as part of the UNSW ZZSC9020 Capstone Data Science Project. This dedicated space serves as the primary platform for collaboration, development, and sharing of resources among our team members.

We utilize this space to facilitate seamless communication and exchange of code, documentation, datasets, models, and other critical resources crucial for the success of our project.

## Group and project information

### Group members and zIDs
- Member 1 (z5135363) - Doug Smithers
- Member 2 (z3478832) - Tom Bernstein
- Member 3 (z5350306) - Daniel Sartor

### Brief project description

Forecasting electrical load is critical for electricity generators and distributors to efficiently dispatch the load, schedule energy transfers, and plan contingencies. Accurate forecasting can significantly lower operational costs, prevent outages, and reduce emissions. Short-term load forecasting (STLF), over a 1-24 hour period, is imperative in managing operations, and STLF will be the focus of this study. Many techniques have been applied to this task. However, the application of neural networks (NNs) is salient, as they do not require the estimation of a complex load model and, crucially, have shown superior forecast accuracy compared to other methods. Since their first use in the late 1980s, advances in NN architectures have offered improved performance and prediction accuracy. This study will compare a simple feed-forward Muli-Layer Perceptron (MLP) NN to a more complex architecture in the Bi-Directional Long Short-Term Memory (BD-LSTM) network to assess the performance gains that come with these advances. In addition, a univariate model will be compared to a multivariate model for each architecture to assess the impact of including additional features. Finally, the model depth of each architecture will be increased to see if performance gains come from allowing the models to find more complex relationships between attributes. While increasing model complexity may come with performance gains, it will also come with increased computational load, which will also be examined. The model results will be compared to predictions by a model supplied by the AEMO Market Management System, which will be used as a baseline for performance. The results from this study have shown that the increased training time required for the complex models prevented them from outperforming the simple MLPs in the timeframe of this project, and more time must be given to training to come to a definitive conclusion. Multivariate models were shown to outperform their univariate equivalents on greater prediction horizons, with the added attributes providing more information for the models to learn relationships. The results showed no clear benefit from increasing model depth. Finally, the results could not match those from the baseline model, and more testing is needed for the applied models to be competitive.

## Repository structure

The repository has the following folder structure:

- agendas: agendas for each weekly meeting with lecturers (left 24h before the next meeting)
- checklists: This folder includes a link our Microsft Planner board used to manage tasks on the project
- data: Includes raw data in it's compressed form
- data-analysis: This folder includes the code for data anlysis carried out on the project including summary statistics and visualisations
- feature-engineering: This module was used to perform feature engineering on the raw data sources
- gantt_chart: Gantt chart or a link to an account in a project task management tool
- minutes: minutes for each meeting (left not more than 24h after the corresponding meeting)
- models: various outputs from the modelling process
- process-flows: The code to generate process flows for the project and the resulting diagrams
- report: PDF of the final report
- Results: experimental result data output by the Modelling notebook
