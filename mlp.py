import torch
import torch.nn as nn



class MLP(nn.Module):

    def __init__(self, input_dim=8, output_dim=3):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, 1028),
            #nn.ReLU(),
            nn.LeakyReLU(negative_slope=0.2),
            nn.BatchNorm1d(1028),
            nn.Dropout(0.2),
            
            nn.Linear(1028, 512),
            #nn.ReLU(),
            nn.LeakyReLU(negative_slope=0.2),
            nn.BatchNorm1d(512),
            nn.Dropout(0.2),
            
            nn.Linear(512, 256),
            #nn.ReLU(),
            nn.LeakyReLU(negative_slope=0.2),
            nn.BatchNorm1d(256),
            nn.Dropout(0.2),
            
            nn.Linear(256, 128),
            #nn.ReLU(),
            nn.LeakyReLU(negative_slope=0.2),
            nn.Linear(128, output_dim)
        )
    
    def forward(self, x):
        return self.net(x) 
        

     