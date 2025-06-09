# IrrigationSchedulingModels

This repo includes codes and relevant input file examples for four Irrigation Scheduling Models (ISMs) developed in Zamaria and Arhonditsis (2025) for the purposes of scheduling irrigation operations to be input into a working SWAT model.

Our approach to developing a suite of ISMs for the Lake Erie Basin without sufficient monitored data combines crop-specific irrigation scheduling recommendations from OMAFRA (2004) with soil moisture and evapotranspiration values simulated by SWAT and observed precipitation data to encompass varying degrees of spatio-temporal variability of irrigation applied. In effect, our ISM strategy aims to represent a wide range of irrigation applications given variability in farmer practices and irrigation methods.

Every ISM is structurally different yet constrained by the same input data, including (i) the growing and harvesting dates per crop; (ii) the area of each HRU; (iii) the nominal irrigation depth per crop (i.e., the amount of water typically applied during an irrigation event); and (iv) the irrigation interval per crop (i.e. the period over which the nominal irrigation depth is applied (Table 2). We also assumed an irrigation efficiency of 75% (IRR_EFF), which indicates that 25% of all irrigation water applied is lost before it can be uptaken by crops. The user can edit these input data in each respective algorithm.


All ISM algorithms were developed in python. Codes for the AUTOIRR, DRIPIRR, CON-S and EB-SWC ISMs are located in the Python folder.
The user will also need to create one csv file per crop considered in the study that includes all other management operations that are not irrigation (ex., tillage, fertilizer applications). An example csv is located in the extra_mgt_operations folder.

For more information, please see Zamaria and Arhonditsis (2025). 
Contact: sophia.zamaria@mail.utoronto.ca

Relevant references:
Zamaria, S. A. & Arhonditsis, G. B. (2025). A framework for evaluating irrigation impact on water balance and crop yield under different moisture conditions. [Submitted]; Hydrological Processes
Ontario Federation of Agriculture. (2004). Best Mangement Practices: Irrigation Management. Ontario Ministry of Agriculture and Food.


