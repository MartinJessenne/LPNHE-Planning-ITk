## Automatic Planning Tool

LPNHE-Planning-ITk is a Python script developed to automate operator assignments as part of the international effort to upgrade the Pixel Detector of the ATLAS experiment at the LHC. The tool is designed to optimize the scheduling of tasks and operator assignments based on real-world production constraints, such as raw product availability, task durations, task dependencies, operator skills, and their availability.
Overview

# This project provides an automated solution to:

 - Precisely assign operators according to the number of raw products available.

 - Calculate the time required to complete each task.

 - Respect the dependencies between tasks.

 - Automatically schedule jobs and assign operators based on their skills and availability.

 - Handle unforeseen events and production changes by relying on a production log that tracks the overall state of the production.

 - Schedule operators in realistic unsegmented shifts.

The solution addresses limitations and challenges present in the original script provided by the CERN collaboration. The original script has been completely recoded and improved in this version. For reference, you can view the original CERN project here: itk-module-production-simulator.
Features

 - Dynamic Operator Assignment: Computes operator assignments based on the current production state, raw product input, and task dependencies.

 - Precise Scheduling: Determines the time required for each task and adjusts schedules accordingly.

 - Skill and Availability Aware: Assigns tasks considering the specific skills of operators and their available time slots.

 - Resilient to Changes: Automatically adapts to unforeseen events through a robust production log.

 - Realistic Shift Planning: Implements unsegmented shift scheduling that mirrors real-world production environments.

# Installation

Clone the repository:
    
    git clone https://github.com/MartinJessenne/LPNHE-Planning-ITk.git
    cd LPNHE-Planning-ITk


Create and activate a virtual environment (optional, but recommended):

      python -m venv venv
      source venv/bin/activate    # On Windows: venv\Scripts\activate

Install the required dependencies:

    pip install -r requirements.txt

   If a requirements.txt is not available yet, add the relevant packages (e.g., numpy, pandas, etc.) based on your project’s needs.

Usage

Run the main scheduling script from the command line. For example:

      python main.py

Command-Line Options

Your script may support additional arguments for:

   - Selecting a specific production log file.

   - Adjusting scheduling parameters.

   - Displaying a summary of the operator assignment.

Configuration

Adjust the scheduling parameters and operator availability in the configuration file (e.g., config.json or similar). This file might include:

   - Task durations.

   - Dependencies between tasks.

   - Operator skills and shift patterns.

   - Raw product counts.

Make sure the configuration file reflects the current production requirements for optimal scheduling results.
Production Log

The production log is a key component of this system. It records:

   - The total production state.

   - Changes and unforeseen events during production.

   - Historical data that is used by the algorithm for future scheduling predictions.

Ensure that your log file is updated and maintained to benefit from the algorithm’s real-time adaptations.
Contributing

Contributions are welcome! If you have suggestions, improvements, or bug fixes, please follow these steps:

   - Fork the repository.

   - Create a new branch for your feature or fix.

   - Commit your changes and push your branch.

   - Open a pull request describing your changes.

For major changes, please open an issue first to discuss what you would like to modify.
License

This project is licensed under the MIT License. See the LICENSE file for details.
Acknowledgements

   - Thanks to the CERN collaboration for the original script as part of the itk-module-production-simulator project.

   - Special thanks to all the collaborators and operators working towards the ATLAS Pixel Detector upgrade.
