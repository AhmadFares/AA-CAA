import numpy as np

qvals_file = "qvalues.npz"
data = np.load(qvals_file, allow_pickle=True)
qvalues = data['qvalues']


stop_action_index = qvalues[0].shape[0] - 1  # Usually last index; adjust if needed

stop_qs = [q[stop_action_index] for q in qvalues]



import matplotlib.pyplot as plt

plt.figure(figsize=(10, 4))
plt.plot(stop_qs, label='Q-value for STOP action')
plt.xlabel("Training step")
plt.ylabel("Q-value (STOP)")
plt.title("Q-value for STOP action over training")
plt.legend()
plt.tight_layout()
plt.show()
