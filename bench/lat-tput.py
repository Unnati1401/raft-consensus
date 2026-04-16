import matplotlib.pyplot as plt
import sys

def parse_results(filename):
    throughputs = []
    latAvg, latP50, latP90, latP99 = [], [], [], []

    try:
        with open(filename, 'r') as f:
            for line in f:
                # Skip header lines
                if line.startswith('#') or line.startswith('-'):
                    continue
                
                parts = line.split()
                if len(parts) >= 6:
                    throughputs.append(float(parts[5]))
                    latAvg.append(float(parts[1]))
                    latP50.append(float(parts[2]))
                    latP90.append(float(parts[3]))
                    latP99.append(float(parts[4]))
    except FileNotFoundError:
        print(f"Error: {filename} not found. Run ./tput first.")
        sys.exit(1)
        
    return throughputs, latAvg, latP50, latP90, latP99

def main():
    throughputs, latAvg, latP50, latP90, latP99 = parse_results('result.txt')
    
    if not throughputs:
        print("No valid data found in result.txt")
        return

    plt.figure(figsize=(10, 6))
    
    plt.plot(throughputs, latAvg, marker='o', label='Average Latency')
    plt.plot(throughputs, latP50, marker='s', label='p50 Latency')
    plt.plot(throughputs, latP90, marker='^', label='p90 Latency')
    plt.plot(throughputs, latP99, marker='x', label='p99 Latency')

    plt.title('Latency vs Throughput')
    plt.xlabel('Throughput (ops/sec)')
    plt.ylabel('Latency (ms)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    plt.savefig('lat-tput.png')
    print("Plot saved to bench/lat-tput.png")

if __name__ == '__main__':
    main()