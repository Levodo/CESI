import math

n =5 
p = 0.6
q = 1 - p

def binomial_probability(n, k, p):
    """
    Calcule P(X=k) pour X ~ Binomiale(n, p).
    Formule: C(n,k) * p^k * (1-p)^(n-k)
    """
    return math.comb(n, k) * (p ** k) * (q ** (n - k))

#calcul détaillé des 3 cas demandés
p3 = binomial_probability(n, 3, p)
p4 = binomial_probability(n, 4, p) 
p5 = binomial_probability(n, 5, p)

# Affichage des résultats
print("Détail des termes (X ~ Bin(n=5, p=0.6)")

for k, pk in [(3, p3), (4, p4), (5, p5)]:
    comb = math.comb(n, k)
    term = f"C({n},{k}) * {p}^{k} * {q}^{n-k}"
    print(f"P(X={k}) = {term} = {comb} * {p**k:.6f} * {q**(n-k):.6f} = {pk:.6f}")

# somme des probabilités
total_probability = p3 + p4 + p5
print(f"\nP(X >= 3) = P(X=3) + P(X=4) + P(X=5) = {p3:.6f} + {p4:.6f} + {p5:.6f} = {total_probability:.6f}")