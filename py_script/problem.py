import torch

class Problem:
    
    def __init__(self, train_X, train_y, ref_point=None):
        self.train_X = train_X
        self.train_y = train_y
        self.dim = train_X.shape[-1]
        self.num_objectives = train_y.shape[-1]
        maxs = []
        mins = []
        for i in range(self.dim):
            maxs.append(max(train_X[:, i]))
            mins.append(min(train_X[:, i]))
        self.bounds = torch.tensor([mins, maxs])
        if ref_point is not None:
            self.ref_point = ref_point
        else:
            ref_point = [min(-train_X[:, 1])]
            ref_point.append(min(train_y[:, -1]))
            self.ref_point = torch.tensor(ref_point, dtype=torch.float64)    
        
