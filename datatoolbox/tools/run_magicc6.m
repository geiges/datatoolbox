% script to run magicc test case
function run_magicc6(varargin)
scenario = varargin{1};
PRIMAP_PATH = varargin{2};
%scenario = 'test3';
%PRIMAP_PATH = '/media/sf_Documents/primap_climatemodule';
scenarioFile = [scenario '.SCEN'];

disp(scenario)
disp(PRIMAP_PATH)


currentPath = pwd;

cd(PRIMAP_PATH)
if ~ exist('PRIMAPDB', 'var')

    startup_CAT_no_clear
end
disp('CAT wrapper initialized')


global para;
para = drive_save_configuration_4MAGICC;
if ~exist(para.magiccinputpath,'dir')
    system(['cp -r ' para.magiccpath '/input ' para.magiccinputpath])
    system(['mkdir ' para.magiccoutputpath])
end
if ~exist(para.magiccoutputpath,'dir')
    system(['mkdir ' scenario])
end
para.runningset = 'run_general';
para.configFileToRun = struct;
para.configFileToRun.DIR = fullfile(para.workingpath, 'config');
para.configFileToRun.RUNCORE = 'run_MAGICC_rocketlauncher_CAT_2018.xlsx';
para.configFileToRun.RUNCORE = 'run_MAGICC_startup_HFC_2019.xls';
%para.configFileToRun.RUNCORE = 'runcore_CA_MAGICC6_single_run.csv';
para.configFileToRun.GENERAL_SETTINGS = 'GENERAL_SETTINGS_2018.CFG';
para.configFileToRun.DEFAULTNAMELIST = 'DEFAULTNAMELIST_CAT_2018.CFG';
para.configFileToRun.DEFAULTYEARS = 'YEARS_CAT_2018.CFG';

para.configFileToRun = 'run_MAGICC_startup_simple.xlsx';
cd(currentPath)



% % files = dir('../scenarios/*.SCEN');
% for file = files'
%     scenario = strrep(file.name, '.SCEN','');    
    % Do some stuff
    % scenario = 'BC_450_KA_MFC_el'

    if ~exist(scenario,'dir')
        system(['mkdir ' scenario])
    end
    
    
%     if contains(scenario,'BC')
%         continue
%     end
    cd (scenario)
    system(['cp -r ../' scenarioFile ' ' para.magiccinputpath '/'])
    system(['cp ' '../' para.configFileToRun ' .'])
    batch_run_magicc_HFC(scenarioFile)
    cd('..')
    close all
exit    
% end

