# Welcome to ZZSC9020 GitHub repository for group K

This GitHub repository is the main point of access for students and lecturers of the ZZSC9020 capstone course. 

In this repository, you will find the data to start developing your project. Also, we will use the repository to share code, documentation, data, models and other resources between the group members and course lecturers.

## Group and project information

### Group members and zIDs
- Member 1 (z5135363) - Doug Smithers
- Member 2 (z3478832) - Tom Bernstein
- Member 3 (z5350306) - Daniel Sartor

### Brief project description

The forecasting of electrical load is critical for electricity generators and distributors to efficiently dispatch the load, schedule energy transfers, and plan for contingencies. Accurate forecasting can save greatly on operational costs, prevent outages and reduce emissions. Short term load forecasting (STLF) (over a 1-24 hour period) is particularly important in managing operations and STLF will be the focus of this study. Many techniques have been applied to this task, but the application of neural networks (NNs) is very prominent as they do not require estimation of a complex load model, and crucially have shown to have superior forecast accuracy to other methods. Advances in NN architectures since their first use in the late 80's have offered improved performance and prediction accuracy. This study will compare a simple feed-forward Muli-Layer Perceptron (MLP) NN to a more complex architecture in the Bi-Directional Long Short Term Memory (BD-LSTM) network to assess the performance gains that come with these advances. In addition to this, a uni-variate model will be compared to a multi-variate model for each architecture, to assess the impact of adding/engineering more features. Finally, the model depth of each architecture will be increased to see if performance gains come from allowing the models to find more complex relationships between attributes. While increasing model complexity may come with performance gains, it will also come with increased computational load and this will also be examined. The model results will be compared to predictions by a model supplied by AEMO Market Management System, which will be used as a baseline for performance. The results from this study have shown that the increased training time required for the complex models prevented them from outperforming the simple MLPs in the timeframe of this project, and more time must be given to training to come to a definitive conclusion. Multi-variate models were shown to outperform their uni-variate equivalents, with the added attributes providing more information for the models to learn relationships. The results showed no clear benefit seen from increasing model depth. Finally, the results were unable to match those from the baseline model, and more testing needs to be done for the applied models to be competitive.

## Repository structure

The repository has the following folder structure:

- agendas: agendas for each weekly meeting with lecturers (left 24h before the next meeting)
- checklists: teamwork checklist or a link to an account in a project task management tool
- data: datasets for analysis
- gantt_chart: Gantt chart or a link to an account in a project task management tool
- minutes: minutes for each meeting (left not more than 24h after the corresponding meeting)
- models: various outputs from the modelling process
- report: PDF of the final report
- Results: experimental result data output by the Modelling notebook
