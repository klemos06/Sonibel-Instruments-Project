# Sonibel-Instruments-Project

Welcome, this repository contains work I have done for firmware, hardware, validation, and embedded systems projects during my time as an Embedded Systems Intern at Sonibel Instruments, a real-time QC welding startup. 

These projects emphasize my interests in linking firmware to hardware by establishing pipelines for data and other telemetry as well as designing custom PCBs to maximize firmware effectiveness.  

I have separated my projects into branches as seen below:

**Branch 1**: Online DAQ Pipeline
- This project consisted of establishing a pipeline from the local database file all the way to an accessible AWS bucket on a Raspberry Pi 5 with an MCC172 IEPE DAQ HAT. It involved using shell scripts and services in Linux to detect power loss, and establishing Internet connection for the parallel data acquisition and backlog upload to the cloud server.
  
**Branch 2**: Analog Filtering and Amplification PCB
- This project was to build a PCB for the filtering of incoming AC current readings and to safely amplify and feed them into an ADC for sampling. The PCB included RC filters accompanied by signal buffers and Butterworth filters.
  
**Branch 3**: Temperature DAQ Pipeline and System Bringup
- This project required the construction of a reliable temperature DAQ pipeline for industrial heat applications. A Photon 2 was used for its IoT capabilites alongside level shifters and a MAX31855 to sample data and wirelessly upload to an AWS S3 bucket using AWS Firehose.
  
**Branch 4 (ongoing)**: System Overhaul onto Toradex Verdin SoM
- This project currently consists of migrating the system onto a more industrial and resilient SoM rather than the Raspberry Pi 5. The SoM consists of a M7 Cortex running FreeRTOS, responsible for IEPE DAQ SPI transactions, and A53 Linux processors, running a Torizon OS for device health monitoring and .db logging. 
